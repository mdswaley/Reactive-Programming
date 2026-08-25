package com.example.learnReactiveProgramming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class LearnError {
    public void learnErrorHandling(){
//        after using try-catch not able to catch the exception
//        bcz after 4s long-running stream the try-catch already gone
//        try {
//            Mono<String> fromCallable = Mono.fromCallable(()->{
//                Thread.sleep(4000);
//                throw new RuntimeException("error occur");
//            });
//
//            fromCallable.subscribe(data -> log.info("data : {}",data));
//        }catch (Exception e){
//            log.error("Caught: ", e);
//        }


        Mono<String> fromCallable = Mono.fromCallable(()->{
            Thread.sleep(4000);
            throw new RuntimeException("error occur");
        });

//        fromCallable
//                .onErrorComplete() // simple ignore the error and continue
//                .subscribe(
//                        data -> log.info("data : {}",data)
//                );

//        fromCallable
//                .onErrorReturn("default fallback") // this will return default value for handling error
//                .subscribe(
//                        data -> log.info("data: {}", data)
//                );

//        Flux.just("apple", "banana", "cherry")
//                .map(fruit -> {
//                    if (fruit.equals("banana")) throw new RuntimeException("Bad fruit!");
//                    return fruit.toUpperCase();
//                })
//                .onErrorResume(err -> Flux.just("mango", "orange")) //  Falling Back to Another Stream
//                .subscribe(System.out::println);

        Flux.just("apple", "banana", "cherry")
                .map(fruit -> {
                    if (fruit.equals("banana")) throw new RuntimeException("Bad fruit!");
                    return fruit.toUpperCase();
                })
                .onErrorMap(err -> new IllegalArgumentException("Failed to process fruits", err))
                        .subscribe(System.out::println,
                                err -> System.err.println("Error: " + err.getMessage())
                        );


        log.info("After callable"); // this will return after 4s. Bcz we are still using synchronous programming only main thread is running.
        // And it was blocked in thread sleep for 4s
    }

}
