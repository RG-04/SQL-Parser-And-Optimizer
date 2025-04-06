%{
#include <stdio.h>
#include <string.h>
#include "sql_parser.tab.h"

void yyerror(const char *s);

/* Keep track of line number for error messages */
int line_num = 1;
%}

%option noyywrap
%option case-insensitive
%option yylineno

%%

[ \t]+          { /* ignore whitespace */ }
[\n\r]          { line_num++; /* track line numbers */ }

SELECT          { return SELECT; }
FROM            { return FROM; }
WHERE           { return WHERE; }
JOIN            { return JOIN; }
ON              { return ON; }
AND             { return AND; }
OR              { return OR; }
AS              { return AS; }
INNER           { return INNER; }
LEFT            { return LEFT; }
RIGHT           { return RIGHT; }
OUTER           { return OUTER; }
CROSS           { return CROSS; }

[a-zA-Z][a-zA-Z0-9_]*  { 
                          yylval.strval = strdup(yytext);
                          return IDENTIFIER; 
                        }

"="             { return EQ; }
"<"             { return LT; }
">"             { return GT; }
"<="            { return LE; }
">="            { return GE; }
"<>"            { return NE; }
"!="            { return NE; }

","             { return COMMA; }
"."             { return DOT; }
"("             { return LPAREN; }
")"             { return RPAREN; }
"*"             { return ASTERISK; }

\'[^']*\'       { 
                  /* Strip quotes for single-quoted strings */
                  yylval.strval = strdup(yytext+1);
                  yylval.strval[strlen(yylval.strval)-1] = '\0';
                  return STRING_LITERAL; 
                }

\"[^"]*\"       {
                  /* Strip quotes for double-quoted strings */
                  yylval.strval = strdup(yytext+1);
                  yylval.strval[strlen(yylval.strval)-1] = '\0';
                  return STRING_LITERAL;
                }

[0-9]+          { 
                  yylval.intval = atoi(yytext);
                  return INTEGER_LITERAL; 
                }

[0-9]+"."[0-9]+ { 
                  yylval.floatval = atof(yytext);
                  return FLOAT_LITERAL; 
                }

--.*$           { /* SQL style comment, ignore */ }
\/\*            { /* Begin C-style comment */
                  int c;
                  while((c = input()) != 0) {
                    if(c == '*') {
                      if((c = input()) == '/')
                        break;
                      else
                        unput(c);
                    } else if(c == '\n') {
                      line_num++;
                    }
                  }
                }

.               { printf("Lexical error on line %d: Unrecognized character: %s\n", line_num, yytext); }

%%