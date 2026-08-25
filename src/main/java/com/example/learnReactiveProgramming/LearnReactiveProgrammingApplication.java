package com.example.learnReactiveProgramming;

import lombok.RequiredArgsConstructor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@RequiredArgsConstructor
public class LearnReactiveProgrammingApplication implements CommandLineRunner {

    private final LearnReactor learnReactor;
    private final LearnOperator learnOperator;
    private final LearnError learnError;

	public static void main(String[] args) {
		SpringApplication.run(LearnReactiveProgrammingApplication.class, args);
	}

    @Override
    public void run(String... args) throws Exception {
//        learnReactor.learReactor();
//        learnOperator.learnMaps();
//        learnOperator.learnAggregatingFlux();
//        learnOperator.learnAggregatingMono();
          learnError.learnErrorHandling();
    }
}
