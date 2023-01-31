package com.transamerica.asyncreadwriteopration;

import com.transamerica.asyncreadwriteopration.service.EmployeeService;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class AsyncReadWriteOprationApplication {

	public static void main(String[] args) throws Exception {

		EmployeeService employeeService  = new EmployeeService();

		CompletableFuture<Void> completableFuture = employeeService.processEmployeeWithDepartmentInCSVFile();

		System.out.println(completableFuture);

		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		URL url = loader.getResource("departments.csv");

		System.out.println(url);
	}
}
