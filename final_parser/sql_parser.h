#ifndef SQL_PARSER_H
#define SQL_PARSER_H

/* Token definitions will be included from y.tab.h */

/* Data structures */
typedef enum {
    OP_PROJECT,
    OP_SELECT,
    OP_JOIN,
    OP_RENAME
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
    } op;
    Table *tables; /* For base relations only */
} RelNode;

/* YYSTYPE is defined in y.tab.h which should be included before this header */
extern YYSTYPE yylval;

#endif /* SQL_PARSER_H */