#ifndef SQL_PARSER_H
#define SQL_PARSER_H

/* Token definitions will be included from y.tab.h */

/* Data structures */
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
    char *table;     // Table or alias name
    char *attr;      // Attribute name (can include dots for subquery columns)
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
            char *table;   // Left side table or alias
            char *attr;    // Left side attribute (can include dots)
            int int_literal;
            float float_literal;
            char *str_literal;
            int literal_type; /* 0: int, 1: float, 2: string, 3: column */
            char *cmp_table;  // Right side table or alias (for column comparisons)
            char *cmp_attr;   // Right side attribute (can include dots)
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

/* Helper functions for handling dotted attribute names */
char* get_first_part(const char* dotted_str);     // Returns the part before the first dot
char* get_remaining_part(const char* dotted_str); // Returns everything after the first dot

/* YYSTYPE is defined in y.tab.h which should be included before this header */
extern YYSTYPE yylval;

#endif /* SQL_PARSER_H */