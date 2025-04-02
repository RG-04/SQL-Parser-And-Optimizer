%{
#include <stdio.h>
#include <string.h>
#include "sql_parser.tab.h"

void yyerror(const char *s);
%}

%option noyywrap
%option case-insensitive

%%

SELECT      { return SELECT; }
FROM        { return FROM; }
WHERE       { return WHERE; }
JOIN        { return JOIN; }
ON          { return ON; }
AND         { return AND; }
OR          { return OR; }
INNER       { return INNER; }
LEFT        { return LEFT; }
RIGHT       { return RIGHT; }
FULL        { return FULL; }
OUTER       { return OUTER; }
AS          { return AS; }

[a-zA-Z][a-zA-Z0-9_]*  { 
                          yylval.strval = strdup(yytext);
                          return IDENTIFIER; 
                        }

"="         { return EQ; }
"<"         { return LT; }
">"         { return GT; }
"<="        { return LE; }
">="        { return GE; }
"<>"        { return NE; }
"!="        { return NE; }

","         { return COMMA; }
"."         { return DOT; }
"("         { return LPAREN; }
")"         { return RPAREN; }
"*"         { return ASTERISK; }

\'[^']*\'   { 
              yylval.strval = strdup(yytext+1);
              yylval.strval[strlen(yylval.strval)-1] = '\0';
              return STRING_LITERAL; 
            }

[0-9]+      { 
              yylval.intval = atoi(yytext);
              return INTEGER_LITERAL; 
            }

[0-9]+"."[0-9]+ { 
                   yylval.floatval = atof(yytext);
                   return FLOAT_LITERAL; 
                 }

[ \t\n\r]   { /* ignore whitespace */ }

.           { printf("Unknown character: %s\n", yytext); }

%%