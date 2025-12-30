#include "explain.h"
#include "check.h"
#include "optimizer.h"
#include "log.h"
#include "instance.h"

/* Define index name. 
 * Three cases:
 * (1) If hit main index, return "primary".
 * (2) If hit user index, return index name.
 * (3) If not hit any, return "none". */
static char *DefineSelectPlanIndexName(SelectPlan *select_plan) {
    if (select_plan->indexValid) return "primary";
    else if (select_plan->hit_index) return select_plan->meta_index->index_name;
    else return "none";
}

/* Execute statement. 
 * This function is simple, just generate select plan and output it.
 * */
static List *ExplainStatement(ExplainNode *explain_node) {
    List *list;
    SelectPlan *select_plan;

    list = create_list(NODE_KEY_VALUE);
    select_plan = OptimizeSelect(explain_node->select_node, SELECT_STMT);
    Assert(select_plan != NULL);

    append_list(list, new_simple_key_value("stmt_type",  "select", T_VARCHAR));
    append_list(list, new_simple_key_value("index",  DefineSelectPlanIndexName(select_plan), T_VARCHAR));
    append_list(list, new_simple_key_value("only_count",  &select_plan->onlyCount, T_BOOL));
    append_list(list, new_simple_key_value("only_scan",  &select_plan->onlyScanIndex, T_BOOL));

    return list;
}

/* Execute explain statement. */
void ExecuteExplainStatement(ExplainNode *explain_node, DBResult *result) {
    Assert(explain_node != NULL);
    if (!CheckForExplain(explain_node)) return;
    result->success = true;
    result->data = ExplainStatement(explain_node);
    result->message = dstrdup("Explain excuted successfully.");
    db_log(SUCCESS, "Explain excuted successfully.");
}
