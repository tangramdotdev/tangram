create or replace procedure handle_queue(
	batch_size int8,
	out n int8
)
language plpgsql
as $$
declare
	p int8;
begin
	n := batch_size;

	if n > 0 then
		call handle_queue_stored_object(n);
	end if;

	if n > 0 then
		call handle_queue_stored_process(n);
	end if;

	if n > 0 then
		call handle_queue_reference_count_cache_entry(n);
	end if;

	if n > 0 then
		call handle_queue_reference_count_object(n);
	end if;

	if n > 0 then
		call handle_queue_reference_count_process(n);
	end if;

	n := batch_size - n;

	if n > 0 then
		update transaction_id set id = id + 1;
	end if;
end;
$$;
