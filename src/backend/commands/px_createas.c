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
#include "tcop/pquery.h"
#include "access/printtup.h"
#include "parser/analyze.h"
#include "utils/ps_status.h"
#include "tcop/utility.h"

static ObjectAddress create_ctas_nodata(List *tlist, IntoClause *into);
static int errdetail_abort(void);
static void executeSQL(const char *sql);
static bool IsTransactionExitStmt(Node *parsetree);
static ObjectAddress create_empty_matview(List *tlist, IntoClause *into);
static void executeSQL(const char *sql);

/*
 * create_empty_matview_internal
 *
 * Internal utility used for the creation of the definition of a materialized view.
 * Caller needs to provide a list of attributes (ColumnDef nodes).
 */
static ObjectAddress
create_empty_matview_internal(List *attrList, IntoClause *into)
{
	CreateStmt *create = makeNode(CreateStmt);
	char relkind = RELKIND_MATVIEW;
	Datum toast_options;
	static char *validnsps[] = HEAP_RELOPT_NAMESPACES;
	ObjectAddress intoRelationAddr;
	Query *query;

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

	/* Create the "view" part of a materialized view. */
	/* StoreViewQuery scribbles on tree, so make a copy */
	query = (Query *)copyObject(into->viewQuery);
	StoreViewQuery(intoRelationAddr.objectId, query, false);
	CommandCounterIncrement();

	return intoRelationAddr;
}

/*
 * errdetail_abort
 *
 * Add an errdetail() line showing abort reason, if any.
 */
static int
errdetail_abort(void)
{
	if (MyProc->recoveryConflictPending)
		errdetail("abort reason: recovery conflict");

	return 0;
}

/* Test a bare parsetree */
static bool
IsTransactionExitStmt(Node *parsetree)
{
	if (parsetree && IsA(parsetree, TransactionStmt))
	{
		TransactionStmt *stmt = (TransactionStmt *)parsetree;

		if (stmt->kind == TRANS_STMT_COMMIT ||
			stmt->kind == TRANS_STMT_PREPARE ||
			stmt->kind == TRANS_STMT_ROLLBACK ||
			stmt->kind == TRANS_STMT_ROLLBACK_TO)
			return true;
	}
	return false;
}

/*
 * create_empty_matview
 *
 * Create an empty materialized view.
 */
static ObjectAddress
create_empty_matview(List *tlist, IntoClause *into)
{
	List *attrList;
	ListCell *t, *lc;

	/*
	 * Build list of ColumnDefs from non-junk elements of the tlist.  If a
	 * column name list was specified in CREATE MATERIALIZED AS, override the column
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
	return create_empty_matview_internal(attrList, into);
}

static void executeSQL(const char *sql)
{
	CommandDest dest = whereToSendOutput;
	List *parsetree_list;
	ListCell *parsetree_item;
	MemoryContext oldcontext;

	/*
	 * Switch to appropriate context for constructing parsetrees.
	 */
	oldcontext = MemoryContextSwitchTo(MessageContext);

	parsetree_list = pg_parse_query(sql);

	MemoryContextSwitchTo(oldcontext);

	foreach (parsetree_item, parsetree_list)
	{
		RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
		const char *commandTag;
		char completionTag[COMPLETION_TAG_BUFSIZE];
		List *querytree_list,
			*plantree_list;
		Portal portal;
		DestReceiver *receiver;
		int16 format;

		/* POLAR px: begin */
		/* reset false every time */
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

		/*
		 * If we are in an aborted transaction, reject all commands except
		 * COMMIT/ABORT.  It is important that this test occur before we try
		 * to do parse analysis, rewrite, or planning, since all those phases
		 * try to do database accesses, which may fail in abort state. (It
		 * might be safe to allow some additional utility commands in this
		 * state, but not many...)
		 */
		if (IsAbortedTransactionBlockState() &&
			!IsTransactionExitStmt(parsetree->stmt))
			ereport(ERROR,
					(errcode(ERRCODE_IN_FAILED_SQL_TRANSACTION),
					 errmsg("current transaction is aborted, "
							"commands ignored until end of transaction block"),
					 errdetail_abort()));

		/* If we got a cancel signal in parsing or prior command, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Set up a snapshot if parse analysis/planning will need one.
		 */
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

		/* Done with the snapshot used for parsing/planning */
		PopActiveSnapshot();

		/* If we got a cancel signal in analysis or planning, quit */
		CHECK_FOR_INTERRUPTS();

		/* Create portal to run the query or queries in. */
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

		/*
		 * We need a CommandCounterIncrement after every query, except
		 * those that start or end a transaction block.
		 */
		CommandCounterIncrement();

		/*
		 * Tell client that we're done with this query.  Note we emit exactly
		 * one EndCommand report for each raw parsetree, thus one for each SQL
		 * command the client sent, regardless of rewriting. (But a command
		 * aborted by error will not send an EndCommand report at all.)
		 */
		EndCommand(completionTag, dest);
	}
}

ObjectAddress px_create_matview(CreateTableAsStmt *stmt, const char *queryString,
								ParamListInfo params, QueryEnvironment *queryEnv, char *completionTag)
{
	Query *query = castNode(Query, stmt->query);
	IntoClause *into = stmt->into;
	bool is_matview = (into->viewQuery != NULL);
	Oid save_userid = InvalidOid;
	int save_sec_context = 0;
	int save_nestlevel = 0;
	ObjectAddress address;
	StringInfo query_string_lower;
	StringInfo sql;
	StringInfo select_clause;
	char *as_clause = " as ";
	char *relname = into->rel->relname;
	Relation intoRelationDesc;
	bool old_px_enable_insert_select;

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
		elog(ERROR, "px_create_matview does not support EXECUTE statement");
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
	GetUserIdAndSecContext(&save_userid, &save_sec_context);
	SetUserIdAndSecContext(save_userid,
						   save_sec_context | SECURITY_RESTRICTED_OPERATION);
	save_nestlevel = NewGUCNestLevel();

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

		/* transform query string to lower case */
		query_string_lower = makeStringInfo();
		for (int i = 0; i < strlen(queryString); i++)
		{
			char c = queryString[i];
			if (c >= 'A' && c <= 'Z')
				appendStringInfoChar(query_string_lower, c - 'A' + 'a');
			else
				appendStringInfoChar(query_string_lower, c);
		}

		set_px_insert_into_matview(true);
		old_px_enable_insert_select = px_enable_insert_select;
		px_enable_insert_select = true;

		/* create an empty materialized view */
		address = create_empty_matview(query->targetList, into);

		/* mark the new materialized view as populated */
		intoRelationDesc = heap_open(address.objectId, AccessExclusiveLock);
		SetMatViewPopulatedState(intoRelationDesc, true);
		heap_close(intoRelationDesc, NoLock);

		/* commit transaction to make the new materialized view visible to woking processes */
		/* pop snapshot that is saved in PortalRunUtility first */
		PopActiveSnapshot();
		CommitTransactionCommand();
		StartTransactionCommand();

		/* temporary solution: assemble a SQL statement */
		sql = makeStringInfo();
		/* extract select clause from the original SQL */
		select_clause = makeStringInfo();
		appendStringInfo(select_clause, "%s", strstr(query_string_lower->data, " as ") + 4);

		appendStringInfo(sql, "insert into %s %s", relname, select_clause->data);

		elog(INFO, "sql: %s", sql->data);

		executeSQL(sql->data);

		px_enable_insert_select = old_px_enable_insert_select;
		set_px_insert_into_matview(false);

		pfree(query_string_lower);
		pfree(select_clause);
		pfree(sql);
	}

	/* Roll back any GUC changes */
	AtEOXact_GUC(false, save_nestlevel);

	/* Restore userid and security context */
	SetUserIdAndSecContext(save_userid, save_sec_context);

	return address;
}
