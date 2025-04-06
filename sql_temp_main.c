#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Function defined in the generated parser */
extern void parse_and_print_plan(FILE *input);

int main(int argc, char **argv) {
    FILE *input = NULL;
    
    printf("SQL Parser with Predicate Pushdown\n");
    printf("----------------------------------\n");
    
    if (argc == 1) {
        /* No file provided, read from stdin */
        printf("Enter SQL query (Ctrl+D to end):\n");
        input = stdin;
    } else if (argc == 2) {
        /* File provided as argument */
        input = fopen(argv[1], "r");
        if (!input) {
            fprintf(stderr, "Error: Could not open file '%s'\n", argv[1]);
            return 1;
        }
    } else {
        fprintf(stderr, "Usage: %s [sql_file]\n", argv[0]);
        return 1;
    }
    
    /* Parse the input and generate/print the plan */
    parse_and_print_plan(input);
    
    /* Close the file if we opened one */
    if (input != stdin) {
        fclose(input);
    }
    
    return 0;
}