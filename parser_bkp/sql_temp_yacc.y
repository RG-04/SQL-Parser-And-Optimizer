%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int yylex();
extern int yyparse();
extern FILE *yyin;
extern int line_num;
extern char *yytext;

void yyerror(const char *s);

/* Structure to represent a relational algebra operation */
typedef struct RelAlgNode {
    enum {
        PROJECTION,
        SELECTION,
        JOIN_OP,
        RELATION
    } type;
    
    char *relation_name;       /* For RELATION */
    char *alias;               /* Table alias */
    char **projection_attrs;   /* For PROJECTION */
    int projection_count;      /* For PROJECTION */
    char *selection_cond;      /* For SELECTION */
    
    struct RelAlgNode *left;   /* Left child */
    struct RelAlgNode *right;  /* Right child (for JOIN) */
    char *join_cond;           /* For JOIN */
    int join_type;             /* 0: INNER, 1: LEFT, 2: RIGHT, 3: OUTER, 4: CROSS */
} RelAlgNode;

/* Functions to create relational algebra nodes */
RelAlgNode* create_relation(char *name, char *alias);
RelAlgNode* create_projection(RelAlgNode *child, char **attrs, int count);
RelAlgNode* create_selection(RelAlgNode *child, char *condition);
RelAlgNode* create_join(RelAlgNode *left, RelAlgNode *right, char *condition, int join_type);

/* Function to concatenate strings */
char* concat_strings(const char *s1, const char *s2);
char* concat_strings3(const char *s1, const char *s2, const char *s3);

/* Function to print relational algebra expression */
void print_rel_algebra(RelAlgNode *node, int level);

/* Function to get the relational algebra expression result */
RelAlgNode* get_rel_algebra_result();

/* Root of the relational algebra tree */
RelAlgNode *result = NULL;
%}

%union {
    int intval;
    float floatval;
    char *strval;
    struct RelAlgNode *node;
    struct {
        char **attrs;
        int count;
    } attr_list;
    struct {
        char *cond;
    } condition;
    int join_type;
}

%token <strval> IDENTIFIER STRING_LITERAL
%token <intval> INTEGER_LITERAL
%token <floatval> FLOAT_LITERAL

%token SELECT FROM WHERE
%token JOIN INNER LEFT RIGHT OUTER CROSS ON
%token AND OR
%token AS
%token COMMA DOT LPAREN RPAREN ASTERISK
%token EQ LT GT LE GE NE

%type <node> query_stmt table_expr join_clause simple_table
%type <attr_list> select_list select_item_list
%type <condition> where_clause opt_where_clause join_condition expr comparison_expr
%type <join_type> join_type opt_join_type
%type <strval> comparison_op column_ref table_alias opt_alias

%left OR
%left AND
%left EQ NE LT GT LE GE

%start sql_stmt

%%

sql_stmt: 
    query_stmt {
        result = $1;
        printf("SQL parsed successfully!\n");
    }
    ;

query_stmt:
    SELECT select_list FROM table_expr opt_where_clause {
        /* Create relational algebra tree */
        RelAlgNode *proj;
        
        if ($5.cond) {
            RelAlgNode *sel = create_selection($4, $5.cond);
            proj = create_projection(sel, $2.attrs, $2.count);
        } else {
            proj = create_projection($4, $2.attrs, $2.count);
        }
        
        $$ = proj;
    }
    ;

select_list:
    ASTERISK {
        $$.attrs = malloc(sizeof(char*));
        $$.attrs[0] = strdup("*");
        $$.count = 1;
    }
    | select_item_list {
        $$ = $1;
    }
    ;

select_item_list:
    column_ref opt_alias {
        $$.attrs = malloc(sizeof(char*));
        if ($2) {
            /* Column with alias */
            $$.attrs[0] = concat_strings3($1, " AS ", $2);
        } else {
            $$.attrs[0] = $1;
        }
        $$.count = 1;
    }
    | select_item_list COMMA column_ref opt_alias {
        $$.count = $1.count + 1;
        $$.attrs = realloc($1.attrs, $$.count * sizeof(char*));
        if ($4) {
            /* Column with alias */
            $$.attrs[$$.count - 1] = concat_strings3($3, " AS ", $4);
        } else {
            $$.attrs[$$.count - 1] = $3;
        }
    }
    ;

column_ref:
    IDENTIFIER {
        $$ = strdup($1);
    }
    | IDENTIFIER DOT IDENTIFIER {
        $$ = concat_strings3($1, ".", $3);
    }
    | IDENTIFIER DOT ASTERISK {
        $$ = concat_strings($1, ".*");
    }
    ;

opt_alias:
    AS IDENTIFIER {
        $$ = strdup($2);
    }
    | IDENTIFIER {
        $$ = strdup($1);
    }
    | /* empty */ {
        $$ = NULL;
    }
    ;

table_expr:
    simple_table {
        $$ = $1;
    }
    | table_expr join_clause {
        /* Set join's left operand to previous table_expr */
        RelAlgNode *join_node = $2;
        join_node->left = $1;
        $$ = join_node;
    }
    ;

simple_table:
    IDENTIFIER table_alias {
        $$ = create_relation($1, $2);
    }
    ;

table_alias:
    AS IDENTIFIER {
        $$ = strdup($2);
    }
    | IDENTIFIER {
        $$ = strdup($1);
    }
    | /* empty */ {
        $$ = NULL;
    }
    ;

join_clause:
    join_type JOIN simple_table ON join_condition {
        $$ = create_join(NULL, $3, $5.cond, $1);
    }
    ;

join_type:
    opt_join_type {
        $$ = $1;
    }
    ;

opt_join_type:
    /* empty */ {
        $$ = 0; /* Default: INNER JOIN */
    }
    | INNER {
        $$ = 0;
    }
    | LEFT opt_outer {
        $$ = 1;
    }
    | RIGHT opt_outer {
        $$ = 2;
    }
    | CROSS {
        $$ = 4;
    }
    ;

opt_outer:
    /* empty */ { }
    | OUTER { }
    ;

join_condition:
    expr {
        $$ = $1;
    }
    ;

opt_where_clause:
    /* empty */ {
        $$.cond = NULL;
    }
    | where_clause {
        $$ = $1;
    }
    ;

where_clause:
    WHERE expr {
        $$ = $2;
    }
    ;

expr:
    comparison_expr {
        $$ = $1;
    }
    | expr AND expr {
        $$.cond = concat_strings3("(", $1.cond, ") AND (");
        $$.cond = concat_strings($$.cond, $3.cond);
        $$.cond = concat_strings($$.cond, ")");
    }
    | expr OR expr {
        $$.cond = concat_strings3("(", $1.cond, ") OR (");
        $$.cond = concat_strings($$.cond, $3.cond);
        $$.cond = concat_strings($$.cond, ")");
    }
    | LPAREN expr RPAREN {
        $$.cond = concat_strings3("(", $2.cond, ")");
    }
    ;

comparison_expr:
    column_ref comparison_op column_ref {
        $$.cond = concat_strings3($1, $2, $3);
    }
    | column_ref comparison_op STRING_LITERAL {
        char *quoted_str = concat_strings3("'", $3, "'");
        $$.cond = concat_strings3($1, $2, quoted_str);
    }
    | column_ref comparison_op INTEGER_LITERAL {
        char int_str[32];
        sprintf(int_str, "%d", $3);
        $$.cond = concat_strings3($1, $2, int_str);
    }
    | column_ref comparison_op FLOAT_LITERAL {
        char float_str[32];
        sprintf(float_str, "%f", $3);
        $$.cond = concat_strings3($1, $2, float_str);
    }
    ;

comparison_op:
    EQ { $$ = strdup(" = "); }
    | LT { $$ = strdup(" < "); }
    | GT { $$ = strdup(" > "); }
    | LE { $$ = strdup(" <= "); }
    | GE { $$ = strdup(" >= "); }
    | NE { $$ = strdup(" <> "); }
    ;

%%

/* Implementation of relational algebra node creation functions */
RelAlgNode* create_relation(char *name, char *alias) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    memset(node, 0, sizeof(RelAlgNode));
    node->type = RELATION;
    node->relation_name = name;
    node->alias = alias ? alias : name;
    return node;
}

RelAlgNode* create_projection(RelAlgNode *child, char **attrs, int count) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    memset(node, 0, sizeof(RelAlgNode));
    node->type = PROJECTION;
    node->projection_attrs = attrs;
    node->projection_count = count;
    node->left = child;
    return node;
}

RelAlgNode* create_selection(RelAlgNode *child, char *condition) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    memset(node, 0, sizeof(RelAlgNode));
    node->type = SELECTION;
    node->selection_cond = condition;
    node->left = child;
    return node;
}

RelAlgNode* create_join(RelAlgNode *left, RelAlgNode *right, char *condition, int join_type) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    memset(node, 0, sizeof(RelAlgNode));
    node->type = JOIN_OP;
    node->join_cond = condition;
    node->join_type = join_type;
    node->left = left;
    node->right = right;
    return node;
}

/* String concatenation helper functions */
char* concat_strings(const char *s1, const char *s2) {
    char *result = malloc(strlen(s1) + strlen(s2) + 1);
    strcpy(result, s1);
    strcat(result, s2);
    return result;
}

char* concat_strings3(const char *s1, const char *s2, const char *s3) {
    char *result = malloc(strlen(s1) + strlen(s2) + strlen(s3) + 1);
    strcpy(result, s1);
    strcat(result, s2);
    strcat(result, s3);
    return result;
}

/* Function to print relational algebra expression */
void print_rel_algebra(RelAlgNode *node, int level) {
    int i;
    if (!node) {
        return;
    }
    
    for (i = 0; i < level; i++) printf("  ");
    
    switch (node->type) {
        case RELATION:
            printf("RELATION(%s", node->relation_name);
            if (node->alias && strcmp(node->relation_name, node->alias) != 0) {
                printf(" AS %s", node->alias);
            }
            printf(")\n");
            break;
            
        case PROJECTION:
            printf("PROJECT(");
            for (i = 0; i < node->projection_count; i++) {
                printf("%s", node->projection_attrs[i]);
                if (i < node->projection_count - 1) printf(", ");
            }
            printf(")\n");
            print_rel_algebra(node->left, level + 1);
            break;
            
        case SELECTION:
            printf("SELECT(%s)\n", node->selection_cond);
            print_rel_algebra(node->left, level + 1);
            break;
            
        case JOIN_OP:
            printf("JOIN(");
            switch (node->join_type) {
                case 0: printf("INNER"); break;
                case 1: printf("LEFT"); break;
                case 2: printf("RIGHT"); break;
                case 3: printf("OUTER"); break;
                case 4: printf("CROSS"); break;
                default: printf("UNKNOWN"); break;
            }
            printf(", %s)\n", node->join_cond);
            
            print_rel_algebra(node->left, level + 1);
            print_rel_algebra(node->right, level + 1);
            break;
    }
}

RelAlgNode* get_rel_algebra_result() {
    return result;
}

void yyerror(const char *s) {
    fprintf(stderr, "Parse error at line %d: %s\nNear text: '%s'\n", 
            line_num, s, yytext);
}

/* Helper function that can be called from main() */
void parse_and_print_plan(FILE *input) {
    yyin = input;
    
    /* Parse the input */
    if (yyparse() != 0) {
        fprintf(stderr, "Parsing failed\n");
        return;
    }
    
    /* Get the result and print it */
    RelAlgNode *plan = get_rel_algebra_result();
    if (plan) {
        printf("\nRelational Algebra Plan:\n");
        print_rel_algebra(plan, 0);
    } else {
        printf("No plan was generated\n");
    }
}