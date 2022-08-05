#include "postgres.h"

#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "catalog/namespace.h"
#include "catalog/toasting.h"
#include "commands/matview.h"
#include "commands/prepare.h"
#include "commands/tablecmds.h"
#include "commands/view.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_clause.h"
#include "rewrite/rewriteHandler.h"
#include "storage/smgr.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/rls.h"
#include "utils/snapmgr.h"
#include "executor/spi.h"
#include "executor/spi_priv.h"
#include "nodes/pg_list.h"

static ObjectAddress create_ctas_nodata(List *tlist, IntoClause *into);

/*
 * create_ctas_internal
 *
 * Internal utility used for the creation of the definition of a relation
 * created via CREATE TABLE AS or a materialized view.  Caller needs to
 * provide a list of attributes (ColumnDef nodes).
 */
static ObjectAddress
create_empty_table_internal(List *attrList, IntoClause *into)
{
	CreateStmt *create = makeNode(CreateStmt);
	char relkind = RELKIND_RELATION;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress intoRelationAddr;

	/*
	 * Create the target relation by faking up a CREATE TABLE parsetree and
	 * passing it to DefineRelation.
	 */
	create->relation = into->rel;
	create->tableElts = attrList;
	create->inhRelations = NIL;
	create->ofTypename = NULL;
	create->constraints = NIL;
	create->options = into->options;
	create->oncommit = into->onCommit;
	create->tablespacename = into->tableSpaceName;
	create->if_not_exists = false;

	/*
	 * Create the relation.  (This will error out if there's an existing view,
	 * so we don't need more code to complain if "replace" is false.)
	 */
	intoRelationAddr = DefineRelation(create, relkind, InvalidOid, NULL, NULL);

	/*
	 * If necessary, create a TOAST table for the target table.  Note that
	 * NewRelationCreateToastTable ends with CommandCounterIncrement(), so
	 * that the TOAST table will be visible for insertion.
	 */
	CommandCounterIncrement();

	/* parse and validate reloptions for the toast table */
	toast_options = transformRelOptions((Datum)0,
										create->options,
										"toast",
										validnsps,
										true, false);

	(void)heap_reloptions(RELKIND_TOASTVALUE, toast_options, true);

	NewRelationCreateToastTable(intoRelationAddr.objectId, toast_options);

	return intoRelationAddr;
}

/*
 * create_empty_table
 *
 * Create an empty table.
 */
static ObjectAddress
create_empty_table(List *tlist, IntoClause *into)
{
	List *attrList;
	ListCell *t, *lc;

	/*
	 * Build list of ColumnDefs from non-junk elements of the tlist.  If a
	 * column name list was specified in CREATE TABLE AS, override the column
	 * names in the query.  (Too few column names are OK, too many are not.)
	 */
	attrList = NIL;
	lc = list_head(into->colNames);
	foreach (t, tlist)
	{
		TargetEntry *tle = (TargetEntry *)lfirst(t);

		if (!tle->resjunk)
		{
			ColumnDef *col;
			char *colname;

			if (lc)
			{
				colname = strVal(lfirst(lc));
				lc = lnext(lc);
			}
			else
				colname = tle->resname;

			col = makeColumnDef(colname,
								exprType((Node *)tle->expr),
								exprTypmod((Node *)tle->expr),
								exprCollation((Node *)tle->expr));

			/*
			 * It's possible that the column is of a collatable type but the
			 * collation could not be resolved, so double-check.  (We must
			 * check this here because DefineRelation would adopt the type's
			 * default collation rather than complaining.)
			 */
			if (!OidIsValid(col->collOid) &&
				type_is_collatable(col->typeName->typeOid))
				ereport(ERROR,
						(errcode(ERRCODE_INDETERMINATE_COLLATION),
						 errmsg("no collation was derived for column \"%s\" with collatable type %s",
								col->colname,
								format_type_be(col->typeName->typeOid)),
						 errhint("Use the COLLATE clause to set the collation explicitly.")));

			attrList = lappend(attrList, col);
		}
	}

	if (lc != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("too many column names were specified")));

	/* Create the relation definition using the ColumnDef list */
	return create_empty_table_internal(attrList, into);
}

void executeSQL(const char *sql)
{
	CommandDest dest = whereToSendOutput;
	List *parsetree_list;
	ListCell *parsetree_item;
	MemoryContext oldcontext;

	parsetree_list = pg_parse_query(sql);
	Assert(list_length(parsetree_list) == 1);
	parsetree_item = linitial_node(ListCell, parsetree_list);

	RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
	bool snapshot_set = false;
	const char *commandTag;
	char completionTag[COMPLETION_TAG_BUFSIZE];
	List *querytree_list,
		*plantree_list;
	Portal portal;
	DestReceiver *receiver;
	int16 format;

	/* POLAR px: begin */
	/* reset false every time*/
	px_use_global_function = false;
	px_is_planning = false;
	px_is_executing = false;
	px_adaptive_paging = px_enable_adaptive_scan;

	/*
	 * Get the command name for use in status display (it also becomes the
	 * default completion tag, down inside PortalRun).  Set ps_status and
	 * do any special start-of-SQL-command processing needed by the
	 * destination.
	 */
	commandTag = CreateCommandTag(parsetree->stmt);

	set_ps_display(commandTag, false);

	BeginCommand(commandTag, dest);

	// /*
	//  * If we are in an aborted transaction, reject all commands except
	//  * COMMIT/ABORT.  It is important that this test occur before we try
	//  * to do parse analysis, rewrite, or planning, since all those phases
	//  * try to do database accesses, which may fail in abort state. (It
	//  * might be safe to allow some additional utility commands in this
	//  * state, but not many...)
	//  */
	// if (IsAbortedTransactionBlockState() &&
	// 	!IsTransactionExitStmt(parsetree->stmt))
	// 	ereport(ERROR,
	// 			(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
	// 			 errmsg("current transaction is aborted, "
	// 					"commands ignored until end of transaction block"),
	// 			 errdetail_abort()));

	/* Make sure we are in a transaction command */
	// start_xact_command();

	/* If we got a cancel signal in parsing or prior command, quit */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Set up a snapshot if parse analysis/planning will need one.
	 */
	// if (analyze_requires_snapshot(parsetree))
	// {
	// 	PushActiveSnapshot(GetTransactionSnapshot());
	// 	snapshot_set = true;
	// }
	PushActiveSnapshot(GetTransactionSnapshot());

	/*
	 * OK to analyze, rewrite, and plan this query.
	 *
	 * Switch to appropriate context for constructing querytrees (again,
	 * these must outlive the execution context).
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

	querytree_list = pg_analyze_and_rewrite(parsetree, sql,
											NULL, 0, NULL);

	plantree_list = pg_plan_queries(querytree_list,
									CURSOR_OPT_PARALLEL_OK | CURSOR_OPT_PX_OK, NULL);

	// /* Done with the snapshot used for parsing/planning */
	// if (snapshot_set)
	PopActiveSnapshot();

	/* If we got a cancel signal in analysis or planning, quit */
	CHECK_FOR_INTERRUPTS();

	/*
	 * Create unnamed portal to run the query or queries in. If there
	 * already is one, silently drop it.
	 */
	portal = CreatePortal("matview_insert_select", true, true);
	/* Don't display the portal in pg_cursors */
	portal->visible = false;

	/*
	 * We don't have to copy anything into the portal, because everything
	 * we are passing here is in MessageContext, which will outlive the
	 * portal anyway.
	 */
	PortalDefineQuery(portal,
					  NULL,
					  sql,
					  T_Invalid, /* POLAR px */
					  commandTag,
					  plantree_list,
					  NULL);

	/*
	 * Start the portal.  No parameters here.
	 */
	PortalStart(portal,
				NULL,
				0,
				InvalidSnapshot,
				NULL /* POALR px */
	);

	/*
	 * Select the appropriate output format: text unless we are doing a
	 * FETCH from a binary cursor.  (Pretty grotty to have to do this here
	 * --- but it avoids grottiness in other places.  Ah, the joys of
	 * backward compatibility...)
	 */
	format = 0; /* TEXT is default */
	if (IsA(parsetree->stmt, FetchStmt))
	{
		FetchStmt *stmt = (FetchStmt *)parsetree->stmt;

		if (!stmt->ismove)
		{
			Portal fportal = GetPortalByName(stmt->portalname);

			if (PortalIsValid(fportal) &&
				(fportal->cursorOptions & CURSOR_OPT_BINARY))
				format = 1; /* BINARY */
		}
	}
	PortalSetResultFormat(portal, 1, &format);

	/*
	 * Now we can create the destination receiver object.
	 */
	receiver = CreateDestReceiver(dest);
	if (dest == DestRemote)
		SetRemoteDestReceiverParams(receiver, portal);

	/*
	 * Switch back to transaction context for execution.
	 */
	MemoryContextSwitchTo(oldcontext);

	/*
	 * Run the portal to completion, and then drop it (and the receiver).
	 */
	(void)PortalRun(portal,
					FETCH_ALL,
					false, /* always top level */
					true,
					receiver,
					receiver,
					completionTag);

	receiver->rDestroy(receiver);

	PortalDrop(portal, false);

	// if (lnext(parsetree_item) == NULL)
	// {
	// 	/*
	// 	 * If this is the last parsetree of the query string, close down
	// 	 * transaction statement before reporting command-complete.  This
	// 	 * is so that any end-of-transaction errors are reported before
	// 	 * the command-complete message is issued, to avoid confusing
	// 	 * clients who will expect either a command-complete message or an
	// 	 * error, not one and then the other.  Also, if we're using an
	// 	 * implicit transaction block, we must close that out first.
	// 	 */
	// 	if (use_implicit_block)
	// 		EndImplicitTransactionBlock();
	// 	finish_xact_command();
	// }
	// else if (IsA(parsetree->stmt, TransactionStmt))
	// {
	// 	/*
	// 	 * If this was a transaction control statement, commit it. We will
	// 	 * start a new xact command for the next command.
	// 	 */
	// 	finish_xact_command();
	// }
	// else
	// {
	// 	/*
	// 	 * We need a CommandCounterIncrement after every query, except
	// 	 * those that start or end a transaction block.
	// 	 */
	// 	CommandCounterIncrement();
	// }

	/*
	 * Tell client that we're done with this query.  Note we emit e
	 ly
	 * one EndCommand report for each raw parsetree, thus one for each SQL
	 * command the client sent, regardless of rewriting. (But a command
	 * aborted by error will not send an EndCommand report at all.)
	 */
	// EndCommand(completionTag, dest);

	/* modify table to make it a materialized view: relkind, finish the view part...  */
}

ObjectAddress px_create_table_as(CreateTableAsStmt *stmt, const char *queryString,
								 ParamListInfo params, QueryEnvironment *queryEnv, char *completionTag)
{
	elog(INFO, "inside px_create_table_as");

	Query *query = castNode(Query, stmt->query);
	IntoClause *into = stmt->into;
	bool is_matview = (into->viewQuery != NULL);
	DestReceiver *dest;
	Oid save_userid = InvalidOid;
	int save_sec_context = 0;
	int save_nestlevel = 0;
	ObjectAddress address;
	List *rewritten;
	PlannedStmt *plan;
	QueryDesc *queryDesc;
	StringInfo sql;

	allow_px_insert_into_matview();

	if (stmt->if_not_exists)
	{
		Oid nspid;

		nspid = RangeVarGetCreationNamespace(stmt->into->rel);

		if (get_relname_relid(stmt->into->rel->relname, nspid))
		{
			ereport(NOTICE,
					(errcode(ERRCODE_DUPLICATE_TABLE),
					 errmsg("relation \"%s\" already exists, skipping",
							stmt->into->rel->relname)));
			return InvalidObjectAddress;
		}
	}

	if (query->commandType == CMD_UTILITY &&
		IsA(query->utilityStmt, ExecuteStmt))
	{
		elog(ERROR, "px_create_table_as does not support EXECUTE statement");
		return InvalidObjectAddress;
	}
	Assert(query->commandType == CMD_SELECT);

	/*
	 * For materialized views, lock down security-restricted operations and
	 * arrange to make GUC variable changes local to this command.  This is
	 * not necessary for security, but this keeps the behavior similar to
	 * REFRESH MATERIALIZED VIEW.  Otherwise, one could create a materialized
	 * view not possible to refresh.
	 */
	if (is_matview)
	{
		GetUserIdAndSecContext(&save_userid, &save_sec_context);
		SetUserIdAndSecContext(save_userid,
							   save_sec_context | SECURITY_RESTRICTED_OPERATION);
		save_nestlevel = NewGUCNestLevel();
	}

	if (into->skipData)
	{
		/*
		 * If WITH NO DATA was specified, do not go through the rewriter,
		 * planner and executor.  Just define the relation using a code path
		 * similar to CREATE VIEW.  This avoids dump/restore problems stemming
		 * from running the planner before all dependencies are set up.
		 */
		address = create_ctas_nodata(query->targetList, into);
	}
	else
	{
		/*
		 * 1. create an empty table, configure its metadata properly
		 * 2. fire up an INSERT INTO ... SELECT ... command to insert tuples into it
		 * 3. modify metadata to make it behave exactly like a matview
		 */
		StringInfo select_clause = makeStringInfo();
		char *start = " as ";
		char *relname = into->rel->relname;
		// int ret;
		// SPIPlanPtr plan;
		// Portal portal;

		elog(INFO, "polar_enable_px: %d, polar_px_enable_insert_select: %d", polar_enable_px, px_enable_insert_select);

		elog(INFO, "relname: %s", relname);

		/* create an empty table */
		address = create_empty_table(query->targetList, into);

		/* make up a SQL statement */
		sql = makeStringInfo();

		/* extract select clause from the original SQL */
		for (int i = 0; i < strlen(queryString) - 4; i++)
		{
			if (strncmp(queryString + i, start, 4) == 0)
			{
				appendStringInfo(select_clause, queryString + i + 4);
				elog(INFO, "select_clause: %s", select_clause->data);
				break;
			}
		}

		appendStringInfo(sql, "insert into %s %s", relname, select_clause->data);
		elog(INFO, "sql: %s", sql->data);

		// /* invoke SPI */
		// if ((ret = SPI_connect()) < 0)
		// 	elog(ERROR, "px_create_table_as (%s): SPI_connect returned %d", relname, ret);

		// if ((plan = SPI_prepare_px(sql->data, 0, NULL)) == NULL)
		// 	elog(ERROR, "SPI_prepare(\"%s\") failed", sql->data);

		// if ((portal = SPI_cursor_open(NULL, plan, NULL, NULL, true)) == NULL)
		// 	elog(ERROR, "SPI_cursor_open(\"%s\") failed", sql->data);

		// elog_node_display(LOG, "plan", linitial_node(PlannedStmt, plan->plancache_list), Debug_pretty_print);

		// SPI_execute_plan(plan, NULL, NULL, false, 0);

		// if ((ret = SPI_execute(sql->data, false, 0)) < 0)
		// 	elog(ERROR, "px_create_table_as (%s): SPI_execute returned %d", relname, ret);

		// SPI_finish();
		executeSQL(sql->data);

		// finish_xact_command();
	}

	if (is_matview)
	{
		/* Roll back any GUC changes */
		AtEOXact_GUC(false, save_nestlevel);

		/* Restore userid and security context */
		SetUserIdAndSecContext(save_userid, save_sec_context);
	}

	return address;
}