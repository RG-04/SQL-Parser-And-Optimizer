#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern FILE *yyin;
extern int yyparse();

/* These declarations are for functions defined in sql_parser.y */
struct RelAlgNode;
void print_rel_algebra(struct RelAlgNode *node, int level);
struct RelAlgNode* get_rel_algebra_result();

int main(int argc, char **argv) {
    FILE *input = NULL;
    
    // Check if input file is provided
    if (argc > 1) {
        input = fopen(argv[1], "r");
        if (!input) {
            fprintf(stderr, "Error: Could not open file '%s'\n", argv[1]);
            return 1;
        }
        yyin = input;
    } else {
        printf("Enter SQL query (end with semicolon on a new line):\n");
        // Use stdin
        yyin = stdin;
    }
    
    // Parse the input
    if (yyparse() == 0) {
        printf("SQL to Relational Algebra conversion completed successfully.\n");
        
        // Get and print the relational algebra tree
        struct RelAlgNode *result = get_rel_algebra_result();
        if (result) {
            printf("\nRelational Algebra Expression:\n");
            print_rel_algebra(result, 0);
        } else {
            printf("No relational algebra expression was generated.\n");
        }
    } else {
        printf("Error parsing SQL statement.\n");
    }
    
    // Close the file if we opened one
    if (input) {
        fclose(input);
    }
    
    return 0;
}