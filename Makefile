CC = gcc
CFLAGS = -Wall -g

all: sql2ra

sql2ra: lex.yy.c sql_parser.tab.c main.c
	$(CC) $(CFLAGS) -o sql2ra lex.yy.c sql_parser.tab.c main.c

lex.yy.c: sql_lexer.l sql_parser.tab.h
	flex sql_lexer.l

sql_parser.tab.c sql_parser.tab.h: sql_parser.y
	bison -d sql_parser.y

clean:
	rm -f sql2ra lex.yy.c sql_parser.tab.c sql_parser.tab.h *.o

test: all
	./sql2ra test.sql

.PHONY: all clean