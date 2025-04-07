#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "y.tab.h"
#include "sql_parser.h"

extern FILE *yyin;
extern int yyparse(void);
extern RelNode *result;
extern void print_ra_tree_json(RelNode *root);
extern void free_relnode(RelNode *node);

void print_usage(char *prog_name) {
    fprintf(stderr, "Usage: %s [sql_file]\n", prog_name);
    fprintf(stderr, "If no file is specified, reads from standard input.\n");
}

int main(int argc, char *argv[]) {
    if (argc > 2) {
        print_usage(argv[0]);
        return 1;
    }
    
    // Input from file or stdin
    if (argc == 2) {
        yyin = fopen(argv[1], "r");
        if (!yyin) {
            fprintf(stderr, "Error: Could not open file '%s'\n", argv[1]);
            return 1;
        }
    } else {
        yyin = stdin;
        printf("Enter SQL query (end with semicolon and newline):\n");
    }
    
    // Parse the input
    if (yyparse() == 0) {
        printf("Parsing successful. Relational Algebra Tree (JSON format):\n");
        if (result != NULL) {
            print_ra_tree_json(result);
            free_relnode(result);  // Clean up memory
        } else {
            printf("Error: No relational algebra tree was generated.\n");
        }
    } else {
        printf("Parsing failed.\n");
    }
    
    if (argc == 2) {
        fclose(yyin);
    }
    
    return 0;
}