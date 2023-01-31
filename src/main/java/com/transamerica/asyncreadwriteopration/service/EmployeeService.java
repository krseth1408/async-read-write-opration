package com.transamerica.asyncreadwriteopration.service;

import com.transamerica.asyncreadwriteopration.entity.Department;
import com.transamerica.asyncreadwriteopration.entity.Employee;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class EmployeeService {

    String EMPLOYEE_CSV_LOC = "C:\\tutorial\\async-read-write-opration\\src\\main\\resources\\employee.csv";
    String DEPT_CSV_LOC = "C:\\tutorial\\async-read-write-opration\\src\\main\\resources\\department.csv";
    String MERGE_CSV_LOC = "C:\\tutorial\\async-read-write-opration\\src\\main\\resources\\EmployeeDepartmentMergeFile.csv";
    private static final String CSV_SEPARATOR = ",";
    Logger logger = Logger.getLogger("EmployeeService.class");


    public CompletableFuture<Void> processEmployeeWithDepartmentInCSVFile() {

        Executor executor = Executors.newFixedThreadPool(3);

        CompletableFuture<Void> voidCompletableFuture = CompletableFuture.supplyAsync(() -> {
            try {
                long departmentProcessingStartTime = System.currentTimeMillis();

                logger.info("");
                List<Department> deptlist = getAllDepartments();

                long departmentProcessingExecutionTime = System.currentTimeMillis() - departmentProcessingStartTime;

                logger.info(" Reading All the departments  - " + Thread.currentThread().getName());
                logger.info(" Department Processing Execution Time : " + departmentProcessingExecutionTime);

                return deptlist;

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, executor).thenApplyAsync((departments) -> {
            long employeeProcessingStartTime = System.currentTimeMillis();
            List<Employee> emps = null;
            try {
                Map<Integer, String> deptIdNameMap = new HashMap<>();
                departments.forEach(depts -> {
                    deptIdNameMap.put(depts.getDepartmentId(), depts.getDepartmentName());
                });
                emps = getAllEmployee();
                if (emps != null && emps.size() > 0) {
                    emps.forEach(e -> {
                        if (e != null && deptIdNameMap.containsKey(e.getDepartmentId())) {
                            e.setDepartmentName(deptIdNameMap.get(e.getDepartmentId()));
                        }
                    });
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long employeeProcessingExecutionTime =  System.currentTimeMillis()-employeeProcessingStartTime;
            logger.info(" Reading and merging the CSV files  - " + Thread.currentThread().getName());
            logger.info(" Employee Processing Execution Time : " + employeeProcessingExecutionTime);

            return emps;

        }, executor).thenAcceptAsync(employees -> {

            try {
                System.out.println(employees);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(MERGE_CSV_LOC), StandardCharsets.UTF_8));
                for (Employee employee : employees) {
                    String oneLine = (employee.getId() <= 0 ? "" : employee.getId()) + CSV_SEPARATOR + (employee.getName().trim().length() == 0 ? "" : employee.getName()) + CSV_SEPARATOR + (employee.getAge() < 0 ? "" : employee.getAge()) + CSV_SEPARATOR + employee.getHireDate() + CSV_SEPARATOR + employee.getDepartmentName();
                    bw.write(oneLine);
                    bw.newLine();
                }
                bw.flush();
                bw.close();
                logger.info(" Creating merge csv file   - " + Thread.currentThread().getName());
            } catch (Exception e) {
                e.printStackTrace();
            }
            long writingToCsvFile  = System.currentTimeMillis();
            long writingToCsvFileExecutionTime =  System.currentTimeMillis()-writingToCsvFile;
            logger.info(" Reading and merging the CSV files  - " + Thread.currentThread().getName());
            logger.info(" Employee Processing Execution Time : " + writingToCsvFileExecutionTime);

        }, executor);
        return voidCompletableFuture;
    }

    public List<Employee> getAllEmployee() throws IOException {
        Optional<List<String>> fileDate = readCSVFileByName(EMPLOYEE_CSV_LOC);
        List<Employee> employees = readEmployees(fileDate);
        return employees;
    }

    public List<Department> getAllDepartments() throws IOException {
        Optional<List<String>> fileData = readCSVFileByName(DEPT_CSV_LOC);
        List<Department> departments = readDepartments(fileData);
        return departments;
    }

    private Optional<List<String>> readCSVFileByName(String file) throws IOException {
        Path path = Paths.get(file);
        Optional<List<String>> fileRows = Optional.of(Files.readAllLines(path));
        return fileRows;
    }


    List<Employee> readEmployees(Optional<List<String>> fileRows) {
        List<Employee> emps = new ArrayList<>();
        Employee employee = null;
        if (fileRows.isPresent()) {
            for (String row : fileRows.get()) {
                employee = new Employee();
                String[] line = row.split(",");
                employee.setId(Integer.parseInt(line[0]));
                employee.setName(line[1]);
                employee.setAge(Integer.parseInt(line[2]));
                employee.setHireDate(line[3]);
                employee.setDepartmentId(Integer.parseInt(line[4]));
                emps.add(employee);
            }
        }
        return emps;
    }

    private List<Department> readDepartments(Optional<List<String>> fileRows) {
        List<Department> depts = new ArrayList<>();
        Department dept = null;
        if (fileRows.isPresent()) {
            for (String row : fileRows.get()) {
                dept = new Department();
                String[] line = row.split(",");
                dept.setDepartmentId(Integer.parseInt(line[0]));
                dept.setDepartmentName(line[1]);
                depts.add(dept);
            }
        }
        return depts;
    }

}
