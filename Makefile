# CC = gcc
# CFLAGS = -Wall -g

# all: sql2ra

# sql2ra: lex.yy.c sql_parser.tab.c main.c
# 	$(CC) $(CFLAGS) -o sql2ra lex.yy.c sql_parser.tab.c main.c

# lex.yy.c: sql_lexer.l sql_parser.tab.h
# 	flex sql_lexer.l

# sql_parser.tab.c sql_parser.tab.h: sql_parser.y
# 	bison -d sql_parser.y

# clean:
# 	rm -f sql2ra lex.yy.c sql_parser.tab.c sql_parser.tab.h *.o

# test: all
# 	./sql2ra test.sql

# .PHONY: all clean

CC = gcc
CFLAGS = -Wall
LEX = flex
YACC = bison

# The main executable
TARGET = sqlparse

# Source files
LEXER_SRC = sql_temp_lex.l
PARSER_SRC = sql_temp_yacc.y
MAIN_SRC = main.c

# Generated files
LEXER_C = lex.yy.c
PARSER_C = sql_parser.tab.c
PARSER_H = sql_parser.tab.h

all: $(TARGET)

$(LEXER_C): $(LEXER_SRC) $(PARSER_H)
	$(LEX) -o $@ $<

$(PARSER_C) $(PARSER_H): $(PARSER_SRC)
	$(YACC) -d -o $(PARSER_C) $<

$(TARGET): $(LEXER_C) $(PARSER_C) $(MAIN_SRC)
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(TARGET) $(LEXER_C) $(PARSER_C) $(PARSER_H) *.o

test: all
	./$(TARGET) test.sql

.PHONY: all clean