CC = gcc
CFLAGS = -Wall -g
LDFLAGS =

PROG = sql_to_ra
OBJECTS = lex.yy.o y.tab.o main.o

all: $(PROG)

$(PROG): $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $(OBJECTS)

lex.yy.c: sql_lexer.l y.tab.h sql_parser.h
	flex sql_lexer.l

y.tab.c y.tab.h: sql_parser.y
	yacc -d sql_parser.y

lex.yy.o: lex.yy.c
	$(CC) $(CFLAGS) -c lex.yy.c

y.tab.o: y.tab.c sql_parser.h
	$(CC) $(CFLAGS) -c y.tab.c

main.o: main.c y.tab.h sql_parser.h
	$(CC) $(CFLAGS) -c main.c

clean:
	rm -f $(PROG) $(OBJECTS) lex.yy.c y.tab.c y.tab.h