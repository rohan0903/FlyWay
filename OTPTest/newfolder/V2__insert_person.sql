SELECT emp.PASSWORD
 ,emp.first_name
 ,dept.department_name
 FROM employees emp
 JOIN departments dept ON (emp.department_id = dept.department_id)
WHERE EXTRACT(MONTH FROM emp.hire_date) = EXTRACT(MONTH FROM SYSDATE);