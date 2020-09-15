pg_config := pg_config
PG_CONFIG_FLAGS = -I$(shell $(pg_config) --includedir) -L$(shell $(pg_config) --libdir)
CFLAGS := -Wall -O2
LDFLAGS := -lpq
CC := cc

pg_logical_cdc: pg_logical_cdc.c postgres_func.h
	$(CC) $(PG_CONFIG_FLAGS) $(CFLAGS) $(LDFLAGS) pg_logical_cdc.c -lpq -o $@

clean:
	rm -f pg_logical_cdc

