--TASK 1
CREATE OR REPLACE PROCEDURE update_employees_rate(rate_change JSON)
LANGUAGE plpgsql
AS $$
    DECLARE
        _salary_after_change integer;
        _employee_id uuid;
        _rate_change numeric(5,2);
        _rec JSON;
    BEGIN
    FOR _rec in SELECT JSON_ARRAY_ELEMENTS(rate_change) LOOP
        _employee_id := (_rec::json ->> 'employee_id')::uuid;
        _rate_change := (100 + (_rec::json ->> 'rate_change')::numeric(5,2))/100;

        SELECT ROUND(employees.rate * _rate_change)
        INTO _salary_after_change
        FROM employees
        WHERE employees.id = _employee_id;

        IF _salary_after_change < 500 THEN _salary_after_change := 500; END IF;

        UPDATE employees SET rate = _salary_after_change WHERE employees.id = _employee_id;
    END LOOP;
    END;
$$;

--TASK 2
CREATE OR REPLACE PROCEDURE indexing_salary(p integer)
LANGUAGE plpgsql
AS $$
    DECLARE
        _avg_salary_before_index integer;
    BEGIN
        SELECT ROUND(SUM(rate)/COUNT(id))
        INTO _avg_salary_before_index
        FROM employees;
        UPDATE employees SET rate = CASE WHEN rate >= _avg_salary_before_index
                                         THEN ROUND(rate * ((100 + p) / 100::numeric(5,2)))
                                         ELSE ROUND(rate * ((102 + p) / 100::numeric(5,2)))
                                    END;
    END;
$$;

--TASK 3
CREATE OR REPLACE PROCEDURE close_project(p uuid)
LANGUAGE plpgsql
AS $$
    DECLARE
        _project_status boolean DEFAULT False;
        _estimated_time integer;
        _fact_time integer;
        _employees text[];
        _bonus_time integer;
        _employee text;
    BEGIN
        SELECT is_active,
               estimated_time
        INTO _project_status,
             _estimated_time
        FROM projects
        WHERE projects.id = p;

        IF NOT _project_status OR _project_status IS NULL THEN RAISE EXCEPTION 'PROJECT HAS ALREADY CLOSED OR TIME NULL'; END IF;

        SELECT SUM(work_hours), array_agg(DISTINCT employee_id)
        INTO _fact_time,
             _employees
        FROM logs
        WHERE project_id = p
        GROUP BY project_id;

        IF _fact_time >= _estimated_time THEN RETURN; END IF;

        _bonus_time := FLOOR((_estimated_time - _fact_time) * 0.75 / (SELECT ARRAY_LENGTH(_employees, 1)));

        IF _bonus_time > 16 THEN _bonus_time:= 16; END IF;

    FOREACH _employee in ARRAY _employees LOOP
        INSERT INTO logs(employee_id, project_id, work_date, work_hours) VALUES (_employee::uuid, p, current_date, _bonus_time);
    END LOOP;

        UPDATE projects SET is_active = False WHERE id = p;
    END;
$$;

--TASK 4
CREATE OR REPLACE PROCEDURE log_work(employee_id uuid, project_id uuid, work_date date, work_hours integer)
LANGUAGE plpgsql
AS $$
    DECLARE
        _required_review boolean DEFAULT False;
    BEGIN
        IF NOT (SELECT is_active FROM projects WHERE id = project_id) THEN RAISE EXCEPTION 'PROJECT HAS ALREADY CLOSED'; END IF;

        IF work_hours > 24 OR work_hours < 1 THEN RAISE NOTICE 'WORK HOURS MUST BE 1 to 24'; RETURN; END IF;

        IF work_hours > 16 OR work_date > current_date OR ((current_date - interval '7 day') > work_date) THEN _required_review := True; END IF;

        INSERT INTO logs(employee_id, project_id, work_date, work_hours, required_review)
               VALUES(employee_id, project_id, work_date, work_hours, _required_review);
    END;
$$;

--TASK 5
CREATE TABLE public.employee_rate_history (
    id uuid,
    employee_id uuid REFERENCES public.employees(id) NOT NULL,
    rate integer NOT NULL CHECK(rate > 0),
    from_date date NOT NULL DEFAULT current_date);

INSERT INTO public.employee_rate_history(employee_id, rate, from_date)
SELECT id, rate, '2020-12-26'::date
FROM employees;

CREATE OR REPLACE FUNCTION save_employee_rate_history()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO employee_rate_history(employee_id, rate) VALUES(NEW.id, NEW.rate);

    RETURN NEW;
END;
$$;

CREATE OR REPLACE TRIGGER change_employee_rate
AFTER
INSERT OR UPDATE OF rate
ON employees
FOR EACH ROW
EXECUTE FUNCTION save_employee_rate_history();

--TASK 6
/*Общая логика работы: Ранжируем в таблице записи черз DENSE_RANK. Далее записываем максимальный ранг
и количество строк. Если максимальный ранг отличается от количества записей, значит есть повторяющиеся значения
и следовательно используем RANDOM в ORDER*/
CREATE OR REPLACE FUNCTION best_project_workers (p_id uuid)
RETURNS TABLE (name text, sum_hours bigint)
LANGUAGE 'plpgsql'
AS $$
DECLARE _max_rank int;
        _count_records int;
BEGIN

    SELECT MAX(dense_rank), COUNT(employee_id)
    INTO _max_rank,
         _count_records
    FROM(SELECT *, DENSE_RANK() OVER (ORDER BY log.sum_hours DESC, log.sum_days DESC) FROM (
        SELECT employee_id, SUM(work_hours) sum_hours, COUNT(DISTINCT work_date) sum_days FROM logs
                    WHERE project_id = p_id
                    GROUP BY project_id, employee_id
                    LIMIT 3) AS log);


    IF _max_rank = _count_records THEN
        RETURN QUERY
        SELECT e.name
        FROM (SELECT employee_id, SUM(work_hours) sum_hours, COUNT(DISTINCT work_date) sum_days FROM logs
                WHERE project_id = p_id
                GROUP BY project_id, employee_id
                ORDER BY sum_hours DESC, sum_days DESC
                LIMIT 3) AS log
        LEFT JOIN employees e ON e.id = log.employee_id;
    ELSE
        RETURN QUERY
        SELECT e.name, log.sum_hours
        FROM (SELECT employee_id, SUM(work_hours) sum_hours, COUNT(DISTINCT work_date) sum_days FROM logs
                WHERE project_id = p_id
                GROUP BY project_id, employee_id
                LIMIT 3) AS log
        LEFT JOIN employees e ON e.id = log.employee_id
        ORDER BY RANDOM ();
    END IF;
END;
$$;

--TASK 7
CREATE OR REPLACE FUNCTION calculate_month_salary(p_date_start date, p_date_end date)
RETURNS TABLE (id uuid,
               name text,
               worked_hours integer,
               salary integer)
LANGUAGE 'plpgsql'
AS $$
DECLARE
    _r record;
BEGIN
        FOR _r IN
                SELECT e.id, e.name, sum::integer worked_hours
                FROM (
                        SELECT SUM(work_hours), employee_id
                        FROM logs l
                        WHERE l.work_date >= p_date_start AND l.work_date <= p_date_end AND required_review IS TRUE AND is_paid IS False
                        GROUP BY employee_id
                      ) AS log
                LEFT JOIN employees e ON e.id = log.employee_id
        LOOP

        RAISE NOTICE 'Warning! Employee % has % hours which must be reviewed!', _r.name, _r.worked_hours;

        END LOOP;

        RETURN QUERY
        SELECT e.id, e.name, sum::integer worked_hours, (sum * rate)::integer salary
        FROM (
                SELECT SUM(work_hours), employee_id
                FROM logs l
                WHERE l.work_date >= p_date_start AND l.work_date <= p_date_end AND (required_review IS False AND is_paid IS False)
                GROUP BY employee_id
                HAVING SUM(work_hours) <= 160
                    UNION
                SELECT ((SUM(work_hours) - 160) * 1.25) + 160, employee_id
                FROM logs l
                WHERE l.work_date >= p_date_start AND l.work_date <= p_date_end AND (required_review IS False AND is_paid IS False)
                GROUP BY employee_id
                HAVING SUM(work_hours) > 160
              ) AS log
        LEFT JOIN employees e ON e.id = log.employee_id;
END;
$$;
