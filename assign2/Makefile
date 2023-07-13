assign2: dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_1.o
	gcc -o assign2 dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_1.o
	
test_assign2_1.o: test_assign2_1.c
	gcc -c test_assign2_1.c
	
storage_mgr.o: storage_mgr.c
	gcc -c storage_mgr.c

dberror.o: dberror.c
	gcc -c dberror.c

buffer_mgr_stat.o: buffer_mgr_stat.c
	gcc -c buffer_mgr_stat.c

buffer_mgr.o: buffer_mgr.c
	gcc -c buffer_mgr.c
	
run: assign2
	./assign2
	
clean:
	-rm assign2 dberror.o storage_mgr.o buffer_mgr_stat.o buffer_mgr.o test_assign2_1.o
