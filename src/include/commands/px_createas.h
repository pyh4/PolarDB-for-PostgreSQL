#ifndef PX_CREATEAS_H
#define PX_CREATEAS_H

#include "catalog/objectaddress.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "tcop/dest.h"
#include "utils/queryenvironment.h"

#ifdef USE_PX

extern ObjectAddress px_create_table_as(CreateTableAsStmt *stmt, const char *queryString,
                                        ParamListInfo params, QueryEnvironment *queryEnv, char *completionTag);

#else

static ObjectAddress
px_create_table_as(CreateTableAsStmt *stmt, const char *queryString,
                   ParamListInfo params, QueryEnvironment *queryEnv, char *completionTag);

static ObjectAddress
px_create_table_as(CreateTableAsStmt *stmt, const char *queryString,
                   ParamListInfo params, QueryEnvironment *queryEnv, char *completionTag)
{
    elog(ERROR, "not support PX create table as in non-px mode");
    return InvalidObjectAddress;
}

#endif

#endif /* PX_CREATEAS_H */