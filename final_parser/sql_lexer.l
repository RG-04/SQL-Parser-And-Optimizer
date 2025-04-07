%{
#include <stdio.h>
#include <string.h>
#include "y.tab.h"
#include "sql_parser.h"

void yyerror(const char *s);
%}

%%

SELECT          { return SELECT; }
FROM            { return FROM; }
WHERE           { return WHERE; }
JOIN            { return JOIN; }
ON              { return ON; }
AS              { return AS; }
AND             { return AND; }
OR              { return OR; }
NOT             { return NOT; }
"="             { return EQ; }
"<"             { return LT; }
">"             { return GT; }
"<="            { return LE; }
">="            { return GE; }
"<>"            { return NE; }
"("             { return '('; }
")"             { return ')'; }
","             { return ','; }
"."             { return '.'; }
"*"             { return '*'; }
"+"             { return '+'; }
"-"             { return '-'; }

[0-9]+          { 
                  yylval.intval = atoi(yytext);
                  return INT_LITERAL;
                }

[0-9]+\.[0-9]+  {
                  yylval.floatval = atof(yytext);
                  return FLOAT_LITERAL;
                }

'[^']*'         {
                  yytext[yyleng-1] = '\0';  /* remove the trailing ' */
                  yylval.strval = strdup(yytext+1);  /* skip the leading ' */
                  return STRING_LITERAL;
                }

[a-zA-Z][a-zA-Z0-9_]*  {
                  yylval.strval = strdup(yytext);
                  return IDENTIFIER;
                }

[ \t\n\r]       { /* ignore whitespace */ }

.               { yyerror("Invalid character"); }

%%

int yywrap(void) {
    return 1;
}