test_assign1: test_assign1_1.o storage_mgr.o dberror.o
	gcc -o test_assign1 test_assign1_1.o storage_mgr.o dberror.o
	
test_assign1_1.o: test_assign1_1.c
	gcc -c test_assign1_1.c
	
storage_mgr.o: storage_mgr.c
	gcc -c storage_mgr.c

dberror.o: dberror.c
	gcc -c dberror.c
	
run: test_assign1
	./test_assign1
	
clean:
	-rm test assign1 test_assign1_1.o storage_mgr.o dberror.o

