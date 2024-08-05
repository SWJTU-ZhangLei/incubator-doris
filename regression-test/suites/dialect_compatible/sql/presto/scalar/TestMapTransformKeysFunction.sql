set sql_dialect='presto';
set enable_fallback_to_original_planner=false;
set debug_skip_fold_constant=false;
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> NULL); # error: errCode = 2, detailMessage = Syntax error in line 1:	... transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> NULL);	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> k); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> k);	                                ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> v); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> v);	                                ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 0);	                                ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> true); # error: errCode = 2, detailMessage = Syntax error in line 1:	... transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> true);	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 'key'); # error: errCode = 2, detailMessage = Syntax error in line 1:	... transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 'key');	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> k + CAST(v as BIGINT)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VA...	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VA...	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[1, 2, 3, 4], ARRAY[10, 20, 30, 40]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...transform_keys(map(ARRAY[1, 2, 3, 4], ARRAY[10, 20, 30...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2, 3, 4]), (k, v) -> v * v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2, 3, 4]), (k, v) -> k || CAST(v as VARCHAR)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[1, 2, 3], ARRAY[1.0E0, 1.4E0, 1.7E0]), (k, v) -> map(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three'])[k]); # error: errCode = 2, detailMessage = Syntax error in line 1:	...transform_keys(map(ARRAY[1, 2, 3], ARRAY[1.0E0, 1.4E0,...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '2010-05-10 12:34:56.123456789'], ARRAY[1, 2]), (k, v) -> date_add('year', 1, k)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...-05-10 12:34:56.123456789', TIMESTAMP '2010-05-10 12:3...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, NULL, 3, 4]), (k, v) -> k || COALESCE(CAST(v as VARCHAR), '0')); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, N...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26, 27], ARRAY[25, 26, 27]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26, 27], ARRAY[25, 26, 27]...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26, 27], ARRAY[25.5E0, 26.5E0, 27.5E0]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26, 27], ARRAY[25.5E0, 26....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26], ARRAY[false, true]), (k, v) -> k % 2 = 0 OR v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26], ARRAY[false, true]), ...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26, 27], ARRAY['abc', 'def', 'xyz']), (k, v) -> to_base(k, 16) || substr(v, 1, 1)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26, 27], ARRAY['abc', 'def...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26], ARRAY[ARRAY['a'], ARRAY['b']]), (k, v) -> ARRAY[CAST(k AS VARCHAR)] || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26], ARRAY[ARRAY['a'], ARR...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25, 26, 27]), (k, v) -> CAST(k * 2 AS BIGINT) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25,...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25.5E0, 26.5E0, 27.5E0]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.2E0, 26.2E0], ARRAY[false, true]), (k, v) -> CAST(k AS BIGINT) % 2 = 0 OR v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.2E0, 26.2E0], ARRAY[false, true...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY['abc', 'def', 'xyz']), (k, v) -> CAST(k AS VARCHAR) || substr(v, 1, 1)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY['ab...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0], ARRAY[ARRAY['a'], ARRAY['b']]), (k, v) -> ARRAY[CAST(k AS VARCHAR)] || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0], ARRAY[ARRAY['a'],...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[true, false], ARRAY[25, 26]), (k, v) -> if(k, 2 * v, 3 * v)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...nsform_keys(map(ARRAY[true, false], ARRAY[25, 26]), (k...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[false, true], ARRAY[25.5E0, 26.5E0]), (k, v) -> if(k, 2 * v, 3 * v)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(map(ARRAY[false, true], ARRAY[25.5E0, 26.5E...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[true, false], ARRAY[true, NULL]), (k, v) -> if(k, NOT v, v IS NULL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...nsform_keys(map(ARRAY[true, false], ARRAY[true, NULL])...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[false, true], ARRAY['abc', 'def']), (k, v) -> if(k, substr(v, 1, 2), substr(v, 1, 1))); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(map(ARRAY[false, true], ARRAY['abc', 'def']...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[true, false], ARRAY[ARRAY['a', 'b'], ARRAY['x', 'y']]), (k, v) -> if(k, reverse(v), v)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...nsform_keys(map(ARRAY[true, false], ARRAY[ARRAY['a', '...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25, 26, 27]), (k, v) -> length(k) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25, 26...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25.5E0, 26.5E0, 27.5E0]), (k, v) -> length(k) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25.5E0...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b'], ARRAY[false, true]), (k, v) -> k = 'b' OR v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b'], ARRAY[false, true]),...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'x'], ARRAY['bc', 'yz']), (k, v) -> k || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'x'], ARRAY['bc', 'yz']), ...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['x', 'y'], ARRAY[ARRAY['a'], ARRAY['b']]), (k, v) -> k || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['x', 'y'], ARRAY[ARRAY['a'], AR...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25, 26]), (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25.5E0, 26.5E0]), (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[false, true]), (k, v) -> contains(k, 3) AND v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[fa...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY['abc', 'xyz']), (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY['a...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY['a'], ARRAY['a', 'b']]), (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[AR...	                             ^	Encountered: COMMA	Expected: ||	
set debug_skip_fold_constant=true;
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> NULL); # error: errCode = 2, detailMessage = Syntax error in line 1:	... transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> NULL);	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> k); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> k);	                                ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> v); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> v);	                                ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 0); # error: errCode = 2, detailMessage = Syntax error in line 1:	SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 0);	                                ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> true); # error: errCode = 2, detailMessage = Syntax error in line 1:	... transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> true);	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 'key'); # error: errCode = 2, detailMessage = Syntax error in line 1:	... transform_keys(map(ARRAY[], ARRAY[]), (k, v) -> 'key');	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> k + CAST(v as BIGINT)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VA...	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VARCHAR)), (k, v) -> v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(CAST(map(ARRAY[], ARRAY[]) AS MAP(BIGINT,VA...	                             ^	Encountered: ]	Expected: IDENTIFIER	
-- SELECT transform_keys(map(ARRAY[1, 2, 3, 4], ARRAY[10, 20, 30, 40]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...transform_keys(map(ARRAY[1, 2, 3, 4], ARRAY[10, 20, 30...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2, 3, 4]), (k, v) -> v * v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2, 3, 4]), (k, v) -> k || CAST(v as VARCHAR)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, 2...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[1, 2, 3], ARRAY[1.0E0, 1.4E0, 1.7E0]), (k, v) -> map(ARRAY[1, 2, 3], ARRAY['one', 'two', 'three'])[k]); # error: errCode = 2, detailMessage = Syntax error in line 1:	...transform_keys(map(ARRAY[1, 2, 3], ARRAY[1.0E0, 1.4E0,...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[TIMESTAMP '2020-05-10 12:34:56.123456789', TIMESTAMP '2010-05-10 12:34:56.123456789'], ARRAY[1, 2]), (k, v) -> date_add('year', 1, k)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...-05-10 12:34:56.123456789', TIMESTAMP '2010-05-10 12:3...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, NULL, 3, 4]), (k, v) -> k || COALESCE(CAST(v as VARCHAR), '0')); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b', 'c', 'd'], ARRAY[1, N...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26, 27], ARRAY[25, 26, 27]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26, 27], ARRAY[25, 26, 27]...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26, 27], ARRAY[25.5E0, 26.5E0, 27.5E0]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26, 27], ARRAY[25.5E0, 26....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26], ARRAY[false, true]), (k, v) -> k % 2 = 0 OR v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26], ARRAY[false, true]), ...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26, 27], ARRAY['abc', 'def', 'xyz']), (k, v) -> to_base(k, 16) || substr(v, 1, 1)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26, 27], ARRAY['abc', 'def...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25, 26], ARRAY[ARRAY['a'], ARRAY['b']]), (k, v) -> ARRAY[CAST(k AS VARCHAR)] || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ransform_keys(map(ARRAY[25, 26], ARRAY[ARRAY['a'], ARR...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25, 26, 27]), (k, v) -> CAST(k * 2 AS BIGINT) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25,...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25.5E0, 26.5E0, 27.5E0]), (k, v) -> k + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY[25....	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.2E0, 26.2E0], ARRAY[false, true]), (k, v) -> CAST(k AS BIGINT) % 2 = 0 OR v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.2E0, 26.2E0], ARRAY[false, true...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY['abc', 'def', 'xyz']), (k, v) -> CAST(k AS VARCHAR) || substr(v, 1, 1)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0, 27.5E0], ARRAY['ab...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[25.5E0, 26.5E0], ARRAY[ARRAY['a'], ARRAY['b']]), (k, v) -> ARRAY[CAST(k AS VARCHAR)] || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...form_keys(map(ARRAY[25.5E0, 26.5E0], ARRAY[ARRAY['a'],...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[true, false], ARRAY[25, 26]), (k, v) -> if(k, 2 * v, 3 * v)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...nsform_keys(map(ARRAY[true, false], ARRAY[25, 26]), (k...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[false, true], ARRAY[25.5E0, 26.5E0]), (k, v) -> if(k, 2 * v, 3 * v)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(map(ARRAY[false, true], ARRAY[25.5E0, 26.5E...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[true, false], ARRAY[true, NULL]), (k, v) -> if(k, NOT v, v IS NULL)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...nsform_keys(map(ARRAY[true, false], ARRAY[true, NULL])...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[false, true], ARRAY['abc', 'def']), (k, v) -> if(k, substr(v, 1, 2), substr(v, 1, 1))); # error: errCode = 2, detailMessage = Syntax error in line 1:	...sform_keys(map(ARRAY[false, true], ARRAY['abc', 'def']...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[true, false], ARRAY[ARRAY['a', 'b'], ARRAY['x', 'y']]), (k, v) -> if(k, reverse(v), v)); # error: errCode = 2, detailMessage = Syntax error in line 1:	...nsform_keys(map(ARRAY[true, false], ARRAY[ARRAY['a', '...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25, 26, 27]), (k, v) -> length(k) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25, 26...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25.5E0, 26.5E0, 27.5E0]), (k, v) -> length(k) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'ab', 'abc'], ARRAY[25.5E0...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'b'], ARRAY[false, true]), (k, v) -> k = 'b' OR v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'b'], ARRAY[false, true]),...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['a', 'x'], ARRAY['bc', 'yz']), (k, v) -> k || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['a', 'x'], ARRAY['bc', 'yz']), ...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY['x', 'y'], ARRAY[ARRAY['a'], ARRAY['b']]), (k, v) -> k || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...ansform_keys(map(ARRAY['x', 'y'], ARRAY[ARRAY['a'], AR...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25, 26]), (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25.5E0, 26.5E0]), (k, v) -> reduce(k, 0, (s, x) -> s + x, s -> s) + v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[25...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[false, true]), (k, v) -> contains(k, 3) AND v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[fa...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY['abc', 'xyz']), (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v); # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY['a...	                             ^	Encountered: COMMA	Expected: ||	
-- SELECT transform_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[ARRAY['a'], ARRAY['a', 'b']]), (k, v) -> transform(k, x -> CAST(x AS VARCHAR)) || v) # error: errCode = 2, detailMessage = Syntax error in line 1:	...orm_keys(map(ARRAY[ARRAY[1, 2], ARRAY[3, 4]], ARRAY[AR...	                             ^	Encountered: COMMA	Expected: ||	