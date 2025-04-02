%{
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern int yylex();
extern int yyparse();
extern FILE *yyin;

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
    char **projection_attrs;   /* For PROJECTION */
    int projection_count;      /* For PROJECTION */
    char *selection_cond;      /* For SELECTION */
    
    struct RelAlgNode *left;   /* Left child */
    struct RelAlgNode *right;  /* Right child (for JOIN) */
    char *join_cond;           /* For JOIN */
    int join_type;             /* 0: INNER, 1: LEFT, 2: RIGHT, 3: FULL */
} RelAlgNode;

/* Functions to create relational algebra nodes */
RelAlgNode* create_relation(char *name);
RelAlgNode* create_projection(RelAlgNode *child, char **attrs, int count);
RelAlgNode* create_selection(RelAlgNode *child, char *condition);
RelAlgNode* create_join(RelAlgNode *left, RelAlgNode *right, char *condition, int join_type);

/* Function to print relational algebra expression */
void print_rel_algebra(RelAlgNode *node, int level);

/* Function to get the relational algebra expression result */
RelAlgNode* get_rel_algebra_result();

/* Root of the relational algebra tree */
RelAlgNode *result = NULL;

/* Function to get the relational algebra expression result */
RelAlgNode* get_rel_algebra_result() {
    return result;
}
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
%token JOIN INNER LEFT RIGHT FULL OUTER ON
%token AND OR
%token AS
%token COMMA DOT LPAREN RPAREN ASTERISK
%token EQ LT GT LE GE NE

%type <node> query_stmt table_expr join_clause simple_table
%type <attr_list> select_list select_item_list
%type <condition> where_clause opt_where_clause join_condition
%type <join_type> join_type opt_join_type
%type <strval> comparison_op

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
        RelAlgNode *sel = NULL;
        
        if ($5.cond) {
            sel = create_selection($4, $5.cond);
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
    IDENTIFIER {
        $$.attrs = malloc(sizeof(char*));
        $$.attrs[0] = $1;
        $$.count = 1;
    }
    | select_item_list COMMA IDENTIFIER {
        $$.count = $1.count + 1;
        $$.attrs = realloc($1.attrs, $$.count * sizeof(char*));
        $$.attrs[$$.count - 1] = $3;
    }
    ;

table_expr:
    simple_table {
        $$ = $1;
    }
    | table_expr join_clause {
        $$ = $2;
    }
    ;

simple_table:
    IDENTIFIER {
        $$ = create_relation($1);
    }
    | IDENTIFIER AS IDENTIFIER {
        /* For simplicity, we ignore aliases in this basic implementation */
        $$ = create_relation($1);
        free($3);
    }
    | LPAREN query_stmt RPAREN {
        $$ = $2;
    }
    ;

join_clause:
    join_type JOIN simple_table ON join_condition {
        $$ = create_join($<node>0, $3, $5.cond, $1);
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
    | FULL opt_outer {
        $$ = 3;
    }
    ;

opt_outer:
    /* empty */ { }
    | OUTER { }
    ;

join_condition:
    IDENTIFIER EQ IDENTIFIER {
        char condition[256];
        sprintf(condition, "%s = %s", $1, $3);
        $$.cond = strdup(condition);
        free($1);
        free($3);
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
    WHERE IDENTIFIER EQ INTEGER_LITERAL {
        char condition[256];
        sprintf(condition, "%s = %d", $2, $4);
        $$.cond = strdup(condition);
        free($2);
    }
    | WHERE IDENTIFIER EQ STRING_LITERAL {
        char condition[256];
        sprintf(condition, "%s = '%s'", $2, $4);
        $$.cond = strdup(condition);
        free($2);
        free($4);
    }
    | WHERE IDENTIFIER comparison_op IDENTIFIER {
        char condition[256];
        sprintf(condition, "%s %s %s", $2, $3, $4);
        $$.cond = strdup(condition);
        free($2);
        free($4);
    }
    ;

comparison_op:
    EQ { $$ = strdup("="); }
    | LT { $$ = strdup("<"); }
    | GT { $$ = strdup(">"); }
    | LE { $$ = strdup("<="); }
    | GE { $$ = strdup(">="); }
    | NE { $$ = strdup("<>"); }
    ;

%%

/* Implementation of relational algebra node creation functions */
RelAlgNode* create_relation(char *name) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    node->type = RELATION;
    node->relation_name = name;
    node->left = NULL;
    node->right = NULL;
    return node;
}

RelAlgNode* create_projection(RelAlgNode *child, char **attrs, int count) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    node->type = PROJECTION;
    node->projection_attrs = attrs;
    node->projection_count = count;
    node->left = child;
    node->right = NULL;
    return node;
}

RelAlgNode* create_selection(RelAlgNode *child, char *condition) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    node->type = SELECTION;
    node->selection_cond = condition;
    node->left = child;
    node->right = NULL;
    return node;
}

RelAlgNode* create_join(RelAlgNode *left, RelAlgNode *right, char *condition, int join_type) {
    RelAlgNode *node = malloc(sizeof(RelAlgNode));
    node->type = JOIN_OP;
    node->join_cond = condition;
    node->join_type = join_type;
    node->left = left;
    node->right = right;
    return node;
}

/* Function to print relational algebra expression */
void print_rel_algebra(RelAlgNode *node, int level) {
    int i;
    for (i = 0; i < level; i++) printf("  ");
    
    if (!node) {
        printf("NULL\n");
        return;
    }
    
    switch (node->type) {
        case RELATION:
            printf("%s\n", node->relation_name);
            break;
            
        case PROJECTION:
            printf("π_");
            for (i = 0; i < node->projection_count; i++) {
                printf("%s", node->projection_attrs[i]);
                if (i < node->projection_count - 1) printf(", ");
            }
            printf(" (\n");
            print_rel_algebra(node->left, level + 1);
            for (i = 0; i < level; i++) printf("  ");
            printf(")\n");
            break;
            
        case SELECTION:
            printf("σ_%s (\n", node->selection_cond);
            print_rel_algebra(node->left, level + 1);
            for (i = 0; i < level; i++) printf("  ");
            printf(")\n");
            break;
            
        case JOIN_OP:
            printf("(\n");
            print_rel_algebra(node->left, level + 1);
            for (i = 0; i < level; i++) printf("  ");
            
            switch (node->join_type) {
                case 0: printf("⋈_%s", node->join_cond); break;  /* INNER JOIN */
                case 1: printf("⟕_%s", node->join_cond); break;  /* LEFT JOIN */
                case 2: printf("⟖_%s", node->join_cond); break;  /* RIGHT JOIN */
                case 3: printf("⟗_%s", node->join_cond); break;  /* FULL JOIN */
                default: printf("?_%s", node->join_cond); break;  /* UNKNOWN JOIN */
            }
            
            printf(" (\n");
            print_rel_algebra(node->right, level + 1);
            for (i = 0; i < level; i++) printf("  ");
            printf(")\n");
            break;
    }
}

void yyerror(const char *s) {
    fprintf(stderr, "Parse error: %s\n", s);
    exit(1);
}