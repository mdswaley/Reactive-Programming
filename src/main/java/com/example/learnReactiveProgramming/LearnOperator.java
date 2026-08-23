package com.example.learnReactiveProgramming;


import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Component
@Slf4j
public class LearnOperator {

    public void learnAggregating(){
        Flux<String> fruits = Flux.just("apple", "banana", "cherry", "date", "eggs");

        fruits
                .filter(fruit -> fruit.length() > 5)
                .subscribe(
                        (data) -> log.info("Data: {}", data)
                );
    }

    public void learnMaps(){
        Flux<Integer> counts = Flux.range(4, 10); // start from 4 then till 10 count. output is 4..13
//        counts.subscribe(
//                (count) -> log.info("Count: {}", count)
//        );

        Flux<Long> ticks = Flux.interval(Duration.ofSeconds(1)); // gives data in every second
//        ticks.subscribe(
//                (tick) -> log.info("Ticks : {}", tick) // tell to the producer give all data in every second
//        );

        Mono<String> fromCallable = Mono.fromCallable(
                () -> slowTask()
        );

//        Each flux method is created new Flux
        Flux<String> fruits = Flux.just("apple", "banana", "cherry", "date", "eggs");

        Flux<String> flux = fruits.map(fruit -> fruit.toUpperCase())
                .map(upperCaseFruit -> upperCaseFruit.substring(0, 3))
                .flatMap(threeChar -> Flux.just(threeChar+"@123", threeChar+"#456")); // flatMap use for convert Flux<Flux<T>> to Flux<T>
//                .subscribe(
//                        (data) -> log.info("Fruits: {}", data)
//                );

        flux.subscribe(
                (data) -> log.info("Data: {}", data)
        );

    }

    @SneakyThrows // @SneakyThrows is a Lombok annotation used to avoid explicitly handling checked exceptions.
//    without this you either need to add try-catch or throws in method level
    private String slowTask(){
        Thread.sleep(4000);
        return "some Data";
    }
}
