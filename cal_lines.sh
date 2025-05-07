wc -l `find ./include ./ -name '*.cc' -or -name '*.c' -or -name '*.S' -or -name '*.hh' -or -name '*.h' -or -name '*.sql' -or -name 'Makefile'`
file_count=$(echo "$files" | wc -l)