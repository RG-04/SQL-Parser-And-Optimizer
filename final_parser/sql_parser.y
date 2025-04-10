%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

void yyerror(const char *s);
int yylex(void);
extern FILE *yyin;

typedef enum {
    OP_PROJECT,
    OP_SELECT,
    OP_JOIN,
    OP_RENAME,
    OP_SUBQUERY  // New operation type for nested queries
} RelOpType;

typedef enum {
    COND_EQ,
    COND_LT,
    COND_GT,
    COND_LE,
    COND_GE,
    COND_NE,
    COND_AND,
    COND_OR,
    COND_NOT
} CondType;

typedef struct Column {
    char *table;
    char *attr;
    struct Column *next;
} Column;

typedef struct Table {
    char *name;
    char *alias;
    struct Table *next;
} Table;

typedef struct Condition {
    CondType type;
    union {
        struct {
            struct Condition *left;
            struct Condition *right;
        } binary;
        struct {
            struct Condition *cond;
        } unary;
        struct {
            char *table;
            char *attr;
            int int_literal;
            float float_literal;
            char *str_literal;
            int literal_type; /* 0: int, 1: float, 2: string, 3: column */
            char *cmp_table;
            char *cmp_attr;
        } comparison;
    } expr;
} Condition;

typedef struct RelNode {
    RelOpType op_type;
    union {
        struct {
            struct RelNode *input;
            Column *columns;
        } project;
        struct {
            struct RelNode *input;
            Condition *condition;
        } select;
        struct {
            struct RelNode *left;
            struct RelNode *right;
            Condition *condition;
        } join;
        struct {
            struct RelNode *input;
            char *old_name;
            char *new_name;
        } rename;
        struct {
            struct RelNode *subquery;
            char *alias;  // Required alias for the subquery
        } subquery;
    } op;
    Table *tables; /* For base relations only */
} RelNode;

/* Helper functions */
Column *create_column(char *table, char *attr);
Column *append_column(Column *list, Column *new_col);
Table *create_table(char *name, char *alias);
Table *append_table(Table *list, Table *new_table);
Condition *create_comparison(CondType type, char *table, char *attr, int literal_type, 
                            int int_val, float float_val, char *str_val, 
                            char *cmp_table, char *cmp_attr);
Condition *create_binary_condition(CondType type, Condition *left, Condition *right);
Condition *create_unary_condition(CondType type, Condition *cond);
RelNode *create_project_node(RelNode *input, Column *columns);
RelNode *create_select_node(RelNode *input, Condition *condition);
RelNode *create_join_node(RelNode *left, RelNode *right, Condition *condition);
RelNode *create_rename_node(RelNode *input, char *old_name, char *new_name);
RelNode *create_base_relation(Table *tables);
RelNode *create_subquery_node(RelNode *subquery, char *alias);
void print_ra_tree_json(RelNode *root);
void free_columns(Column *cols);
void free_tables(Table *tables);
void free_condition(Condition *cond);
void free_relnode(RelNode *node);

RelNode *result = NULL;
%}

%union {
    int intval;
    float floatval;
    char *strval;
    struct Column *col;
    struct Table *tbl;
    struct Condition *cond;
    struct RelNode *node;
}

%token <strval> IDENTIFIER
%token <intval> INT_LITERAL
%token <floatval> FLOAT_LITERAL
%token <strval> STRING_LITERAL

%token SELECT FROM WHERE JOIN ON AS AND OR NOT
%token EQ LT GT LE GE NE

%type <col> column_list column
%type <tbl> table_list table_ref
%type <cond> where_clause opt_where_clause condition comparison_expr
%type <cond> join_condition
%type <node> query_stmt join_list join_table table_item subquery
%type <strval> dotted_identifier

%left OR
%left AND
%right NOT

%%

start: query_stmt {
    result = $1;
}
;

query_stmt: 
    SELECT column_list FROM join_list opt_where_clause {
        $$ = create_project_node($4, $2);
        if ($5 != NULL) {
            $$ = create_select_node($$, $5);
        }
    }
;

column_list:
    column {
        $$ = $1;
    }
    | column_list ',' column {
        $$ = append_column($1, $3);
    }
;

column:
    IDENTIFIER '.' dotted_identifier {
        $$ = create_column($1, $3);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' '*' {
        $$ = create_column($1, strdup("*"));
        free($1);
    }
;

dotted_identifier:
    IDENTIFIER {
        $$ = strdup($1);
        free($1);
    }
    | dotted_identifier '.' IDENTIFIER {
        char* temp = (char*)malloc(strlen($1) + strlen($3) + 2); // +2 for the dot and null terminator
        sprintf(temp, "%s.%s", $1, $3);
        $$ = temp;
        free($1);
        free($3);
    }
;

join_list:
    table_item {
        $$ = $1;
    }
    | join_list JOIN join_table ON join_condition {
        $$ = create_join_node($1, $3, $5);
    }
;

join_table:
    table_item {
        $$ = $1;
    }
;

table_item:
    table_ref {
        $$ = create_base_relation($1);
    }
    | subquery {
        $$ = $1;
    }
;

subquery:
    '(' query_stmt ')' AS IDENTIFIER {
        $$ = create_subquery_node($2, $5);
        free($5);
    }
    | '(' query_stmt ')' IDENTIFIER {  /* Implicit AS for subquery */
        $$ = create_subquery_node($2, $4);
        free($4);
    }
;

table_ref:
    IDENTIFIER {
        $$ = create_table($1, NULL);
        free($1);
    }
    | IDENTIFIER AS IDENTIFIER {
        $$ = create_table($1, $3);
        free($1);
        free($3);
    }
    | IDENTIFIER IDENTIFIER {  /* Implicit AS */
        $$ = create_table($1, $2);
        free($1);
        free($2);
    }
;

table_list:
    table_ref {
        $$ = $1;
    }
    | table_list ',' table_ref {
        $$ = append_table($1, $3);
    }
;

opt_where_clause:
    /* empty */ {
        $$ = NULL;
    }
    | where_clause {
        $$ = $1;
    }
;

where_clause:
    WHERE condition {
        $$ = $2;
    }
;

join_condition:
    condition {
        $$ = $1;
    }
;

condition:
    comparison_expr {
        $$ = $1;
    }
    | condition AND condition {
        $$ = create_binary_condition(COND_AND, $1, $3);
    }
    | condition OR condition {
        $$ = create_binary_condition(COND_OR, $1, $3);
    }
    | NOT condition {
        $$ = create_unary_condition(COND_NOT, $2);
    }
    | '(' condition ')' {
        $$ = $2;
    }
;

comparison_expr:
    IDENTIFIER '.' dotted_identifier EQ IDENTIFIER '.' dotted_identifier {
        $$ = create_comparison(COND_EQ, $1, $3, 3, 0, 0.0, NULL, $5, $7);
        free($1);
        free($3);
        free($5);
        free($7);
    }
    | IDENTIFIER '.' dotted_identifier EQ INT_LITERAL {
        $$ = create_comparison(COND_EQ, $1, $3, 0, $5, 0.0, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier EQ FLOAT_LITERAL {
        $$ = create_comparison(COND_EQ, $1, $3, 1, 0, $5, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier EQ STRING_LITERAL {
        $$ = create_comparison(COND_EQ, $1, $3, 2, 0, 0.0, $5, NULL, NULL);
        free($1);
        free($3);
        free($5);
    }
    | IDENTIFIER '.' dotted_identifier LT IDENTIFIER '.' dotted_identifier {
        $$ = create_comparison(COND_LT, $1, $3, 3, 0, 0.0, NULL, $5, $7);
        free($1);
        free($3);
        free($5);
        free($7);
    }
    | IDENTIFIER '.' dotted_identifier GT IDENTIFIER '.' dotted_identifier {
        $$ = create_comparison(COND_GT, $1, $3, 3, 0, 0.0, NULL, $5, $7);
        free($1);
        free($3);
        free($5);
        free($7);
    }
    | IDENTIFIER '.' dotted_identifier LE IDENTIFIER '.' dotted_identifier {
        $$ = create_comparison(COND_LE, $1, $3, 3, 0, 0.0, NULL, $5, $7);
        free($1);
        free($3);
        free($5);
        free($7);
    }
    | IDENTIFIER '.' dotted_identifier GE IDENTIFIER '.' dotted_identifier {
        $$ = create_comparison(COND_GE, $1, $3, 3, 0, 0.0, NULL, $5, $7);
        free($1);
        free($3);
        free($5);
        free($7);
    }
    | IDENTIFIER '.' dotted_identifier NE IDENTIFIER '.' dotted_identifier {
        $$ = create_comparison(COND_NE, $1, $3, 3, 0, 0.0, NULL, $5, $7);
        free($1);
        free($3);
        free($5);
        free($7);
    }
    | IDENTIFIER '.' dotted_identifier LT INT_LITERAL {
        $$ = create_comparison(COND_LT, $1, $3, 0, $5, 0.0, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier GT INT_LITERAL {
        $$ = create_comparison(COND_GT, $1, $3, 0, $5, 0.0, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier LE INT_LITERAL {
        $$ = create_comparison(COND_LE, $1, $3, 0, $5, 0.0, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier GE INT_LITERAL {
        $$ = create_comparison(COND_GE, $1, $3, 0, $5, 0.0, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier NE INT_LITERAL {
        $$ = create_comparison(COND_NE, $1, $3, 0, $5, 0.0, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier LT FLOAT_LITERAL {
        $$ = create_comparison(COND_LT, $1, $3, 1, 0, $5, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier GT FLOAT_LITERAL {
        $$ = create_comparison(COND_GT, $1, $3, 1, 0, $5, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier LE FLOAT_LITERAL {
        $$ = create_comparison(COND_LE, $1, $3, 1, 0, $5, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier GE FLOAT_LITERAL {
        $$ = create_comparison(COND_GE, $1, $3, 1, 0, $5, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier NE FLOAT_LITERAL {
        $$ = create_comparison(COND_NE, $1, $3, 1, 0, $5, NULL, NULL, NULL);
        free($1);
        free($3);
    }
    | IDENTIFIER '.' dotted_identifier LT STRING_LITERAL {
        $$ = create_comparison(COND_LT, $1, $3, 2, 0, 0.0, $5, NULL, NULL);
        free($1);
        free($3);
        free($5);
    }
    | IDENTIFIER '.' dotted_identifier GT STRING_LITERAL {
        $$ = create_comparison(COND_GT, $1, $3, 2, 0, 0.0, $5, NULL, NULL);
        free($1);
        free($3);
        free($5);
    }
    | IDENTIFIER '.' dotted_identifier LE STRING_LITERAL {
        $$ = create_comparison(COND_LE, $1, $3, 2, 0, 0.0, $5, NULL, NULL);
        free($1);
        free($3);
        free($5);
    }
    | IDENTIFIER '.' dotted_identifier GE STRING_LITERAL {
        $$ = create_comparison(COND_GE, $1, $3, 2, 0, 0.0, $5, NULL, NULL);
        free($1);
        free($3);
        free($5);
    }
    | IDENTIFIER '.' dotted_identifier NE STRING_LITERAL {
        $$ = create_comparison(COND_NE, $1, $3, 2, 0, 0.0, $5, NULL, NULL);
        free($1);
        free($3);
        free($5);
    }
;

%%

void yyerror(const char *s) {
    fprintf(stderr, "Error: %s\n", s);
}

/* Helper functions for handling dotted attribute names */
char* get_first_part(const char* dotted_str) {
    // Returns the part before the first dot
    char* dot_pos = strchr(dotted_str, '.');
    if (dot_pos == NULL) {
        return strdup(dotted_str); // No dot found, return the whole string
    }
    
    int length = dot_pos - dotted_str;
    char* result = (char*)malloc(length + 1);
    strncpy(result, dotted_str, length);
    result[length] = '\0';
    return result;
}

char* get_remaining_part(const char* dotted_str) {
    // Returns everything after the first dot
    char* dot_pos = strchr(dotted_str, '.');
    if (dot_pos == NULL) {
        return NULL; // No dot found
    }
    
    return strdup(dot_pos + 1); // +1 to skip the dot itself
}

Column *create_column(char *table, char *attr) {
    Column *col = (Column *)malloc(sizeof(Column));
    col->table = strdup(table);
    col->attr = strdup(attr);
    col->next = NULL;
    return col;
}

Column *append_column(Column *list, Column *new_col) {
    if (list == NULL) {
        return new_col;
    }
    
    Column *current = list;
    while (current->next != NULL) {
        current = current->next;
    }
    current->next = new_col;
    return list;
}

Table *create_table(char *name, char *alias) {
    Table *tbl = (Table *)malloc(sizeof(Table));
    tbl->name = strdup(name);
    tbl->alias = (alias != NULL) ? strdup(alias) : NULL;
    tbl->next = NULL;
    return tbl;
}

Table *append_table(Table *list, Table *new_table) {
    if (list == NULL) {
        return new_table;
    }
    
    Table *current = list;
    while (current->next != NULL) {
        current = current->next;
    }
    current->next = new_table;
    return list;
}

Condition *create_comparison(CondType type, char *table, char *attr, int literal_type, 
                            int int_val, float float_val, char *str_val, 
                            char *cmp_table, char *cmp_attr) {
    Condition *cond = (Condition *)malloc(sizeof(Condition));
    cond->type = type;
    cond->expr.comparison.table = strdup(table);
    cond->expr.comparison.attr = strdup(attr);
    cond->expr.comparison.literal_type = literal_type;
    
    if (literal_type == 0) { /* int */
        cond->expr.comparison.int_literal = int_val;
    } else if (literal_type == 1) { /* float */
        cond->expr.comparison.float_literal = float_val;
    } else if (literal_type == 2) { /* string */
        cond->expr.comparison.str_literal = strdup(str_val);
    } else if (literal_type == 3) { /* column */
        cond->expr.comparison.cmp_table = strdup(cmp_table);
        cond->expr.comparison.cmp_attr = strdup(cmp_attr);
    }
    
    return cond;
}

Condition *create_binary_condition(CondType type, Condition *left, Condition *right) {
    Condition *cond = (Condition *)malloc(sizeof(Condition));
    cond->type = type;
    cond->expr.binary.left = left;
    cond->expr.binary.right = right;
    return cond;
}

Condition *create_unary_condition(CondType type, Condition *cond_expr) {
    Condition *cond = (Condition *)malloc(sizeof(Condition));
    cond->type = type;
    cond->expr.unary.cond = cond_expr;
    return cond;
}

RelNode *create_project_node(RelNode *input, Column *columns) {
    RelNode *node = (RelNode *)malloc(sizeof(RelNode));
    node->op_type = OP_PROJECT;
    node->op.project.input = input;
    node->op.project.columns = columns;
    node->tables = NULL;
    return node;
}

RelNode *create_select_node(RelNode *input, Condition *condition) {
    RelNode *node = (RelNode *)malloc(sizeof(RelNode));
    node->op_type = OP_SELECT;
    node->op.select.input = input;
    node->op.select.condition = condition;
    node->tables = NULL;
    return node;
}

RelNode *create_join_node(RelNode *left, RelNode *right, Condition *condition) {
    RelNode *node = (RelNode *)malloc(sizeof(RelNode));
    node->op_type = OP_JOIN;
    node->op.join.left = left;
    node->op.join.right = right;
    node->op.join.condition = condition;
    node->tables = NULL;
    return node;
}

RelNode *create_rename_node(RelNode *input, char *old_name, char *new_name) {
    RelNode *node = (RelNode *)malloc(sizeof(RelNode));
    node->op_type = OP_RENAME;
    node->op.rename.input = input;
    node->op.rename.old_name = strdup(old_name);
    node->op.rename.new_name = strdup(new_name);
    node->tables = NULL;
    return node;
}

RelNode *create_base_relation(Table *tables) {
    RelNode *node = (RelNode *)malloc(sizeof(RelNode));
    node->op_type = (RelOpType)-1; /* Mark as base relation */
    node->tables = tables;
    return node;
}

/* New function to create a subquery node */
RelNode *create_subquery_node(RelNode *subquery, char *alias) {
    RelNode *node = (RelNode *)malloc(sizeof(RelNode));
    node->op_type = OP_SUBQUERY;
    node->op.subquery.subquery = subquery;
    node->op.subquery.alias = strdup(alias);
    node->tables = NULL;
    return node;
}

void print_column_json(Column *col) {
    printf("[");
    while (col != NULL) {
        printf("{\"table\": \"%s\", \"attr\": \"%s\"}", 
               col->table, col->attr);
        col = col->next;
        if (col != NULL) {
            printf(", ");
        }
    }
    printf("]");
}

void print_table_json(Table *tbl) {
    printf("[");
    while (tbl != NULL) {
        printf("{\"name\": \"%s\"", tbl->name);
        if (tbl->alias != NULL) {
            printf(", \"alias\": \"%s\"", tbl->alias);
        }
        printf("}");
        tbl = tbl->next;
        if (tbl != NULL) {
            printf(", ");
        }
    }
    printf("]");
}

void print_condition_json(Condition *cond) {
    if (cond == NULL) {
        printf("null");
        return;
    }
    
    printf("{\"type\": ");
    
    switch (cond->type) {
        case COND_EQ:
            printf("\"EQ\"");
            break;
        case COND_LT:
            printf("\"LT\"");
            break;
        case COND_GT:
            printf("\"GT\"");
            break;
        case COND_LE:
            printf("\"LE\"");
            break;
        case COND_GE:
            printf("\"GE\"");
            break;
        case COND_NE:
            printf("\"NE\"");
            break;
        case COND_AND:
            printf("\"AND\", \"left\": ");
            print_condition_json(cond->expr.binary.left);
            printf(", \"right\": ");
            print_condition_json(cond->expr.binary.right);
            break;
        case COND_OR:
            printf("\"OR\", \"left\": ");
            print_condition_json(cond->expr.binary.left);
            printf(", \"right\": ");
            print_condition_json(cond->expr.binary.right);
            break;
        case COND_NOT:
            printf("\"NOT\", \"cond\": ");
            print_condition_json(cond->expr.unary.cond);
            break;
    }
    
    if (cond->type <= COND_NE) { /* Comparison operation */
        printf(", \"left\": {\"table\": \"%s\", \"attr\": \"%s\"}", 
               cond->expr.comparison.table, cond->expr.comparison.attr);
        
        printf(", \"right\": ");
        
        if (cond->expr.comparison.literal_type == 0) { /* int */
            printf("{\"type\": \"int\", \"value\": %d}", 
                   cond->expr.comparison.int_literal);
        } else if (cond->expr.comparison.literal_type == 1) { /* float */
            printf("{\"type\": \"float\", \"value\": %f}", 
                   cond->expr.comparison.float_literal);
        } else if (cond->expr.comparison.literal_type == 2) { /* string */
            printf("{\"type\": \"string\", \"value\": \"%s\"}", 
                   cond->expr.comparison.str_literal);
        } else if (cond->expr.comparison.literal_type == 3) { /* column */
            printf("{\"type\": \"column\", \"table\": \"%s\", \"attr\": \"%s\"}", 
                   cond->expr.comparison.cmp_table, cond->expr.comparison.cmp_attr);
        }
    }
    
    printf("}");
}

void print_ra_tree_json_rec(RelNode *node) {
    if (node == NULL) {
        printf("null");
        return;
    }
    
    printf("{");
    
    if (node->tables != NULL) { /* Base relation */
        printf("\"type\": \"base_relation\", \"tables\": ");
        print_table_json(node->tables);
    } else {
        switch (node->op_type) {
            case OP_PROJECT:
                printf("\"type\": \"project\", \"columns\": ");
                print_column_json(node->op.project.columns);
                printf(", \"input\": ");
                print_ra_tree_json_rec(node->op.project.input);
                break;
                
            case OP_SELECT:
                printf("\"type\": \"select\", \"condition\": ");
                print_condition_json(node->op.select.condition);
                printf(", \"input\": ");
                print_ra_tree_json_rec(node->op.select.input);
                break;
                
            case OP_JOIN:
                printf("\"type\": \"join\", \"condition\": ");
                print_condition_json(node->op.join.condition);
                printf(", \"left\": ");
                print_ra_tree_json_rec(node->op.join.left);
                printf(", \"right\": ");
                print_ra_tree_json_rec(node->op.join.right);
                break;
                
            case OP_RENAME:
                printf("\"type\": \"rename\", \"old_name\": \"%s\", \"new_name\": \"%s\", \"input\": ", 
                       node->op.rename.old_name, node->op.rename.new_name);
                print_ra_tree_json_rec(node->op.rename.input);
                break;
                
            case OP_SUBQUERY:
                printf("\"type\": \"subquery\", \"alias\": \"%s\", \"query\": ", 
                       node->op.subquery.alias);
                print_ra_tree_json_rec(node->op.subquery.subquery);
                break;
        }
    }
    
    printf("}");
}

void print_ra_tree_json(RelNode *root) {
    print_ra_tree_json_rec(root);
    printf("\n");
}

void free_columns(Column *cols) {
    while (cols != NULL) {
        Column *next = cols->next;
        free(cols->table);
        free(cols->attr);
        free(cols);
        cols = next;
    }
}

void free_tables(Table *tables) {
    while (tables != NULL) {
        Table *next = tables->next;
        free(tables->name);
        if (tables->alias != NULL) {
            free(tables->alias);
        }
        free(tables);
        tables = next;
    }
}

void free_condition(Condition *cond) {
    if (cond == NULL) {
        return;
    }
    
    switch (cond->type) {
        case COND_AND:
        case COND_OR:
            free_condition(cond->expr.binary.left);
            free_condition(cond->expr.binary.right);
            break;
        case COND_NOT:
            free_condition(cond->expr.unary.cond);
            break;
        default: /* Comparison operations */
            free(cond->expr.comparison.table);
            free(cond->expr.comparison.attr);
            
            if (cond->expr.comparison.literal_type == 2) { /* string */
                free(cond->expr.comparison.str_literal);
            } else if (cond->expr.comparison.literal_type == 3) { /* column */
                free(cond->expr.comparison.cmp_table);
                free(cond->expr.comparison.cmp_attr);
            }
            break;
    }
    
    free(cond);
}

void free_relnode(RelNode *node) {
    if (node == NULL) {
        return;
    }
    
    if (node->tables != NULL) { /* Base relation */
        free_tables(node->tables);
    } else {
        switch (node->op_type) {
            case OP_PROJECT:
                free_columns(node->op.project.columns);
                free_relnode(node->op.project.input);
                break;
                
            case OP_SELECT:
                free_condition(node->op.select.condition);
                free_relnode(node->op.select.input);
                break;
                
            case OP_JOIN:
                free_condition(node->op.join.condition);
                free_relnode(node->op.join.left);
                free_relnode(node->op.join.right);
                break;
                
            case OP_RENAME:
                free(node->op.rename.old_name);
                free(node->op.rename.new_name);
                free_relnode(node->op.rename.input);
                break;
                
            case OP_SUBQUERY:
                free(node->op.subquery.alias);
                free_relnode(node->op.subquery.subquery);
                break;
        }
    }
    
    free(node);
}