package com.example.learnReactiveProgramming;


import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Comparator;

@Component
@Slf4j
public class LearnOperator {

    public void learnAggregating(){
        Flux<String> fruits = Flux.just("apple", "banana", "cherry", "date", "eggs");
        Flux<String> moreFruits = Flux.just("pomegranate", "mango");
        Flux<String> colors = Flux.just("red", "yellow", "black", "brown", "white");


        System.out.println("using concat method");
        Flux.concat(fruits, moreFruits) // concat first fruits with moreFruits but need to wait for fruits to call on onComplete()
                        .subscribe(System.out::println);

        System.out.println("using concatWith method");
        fruits.concatWith(moreFruits) // another way. same wait for first to complete
                        .subscribe(System.out::println);

        System.out.println("using mergeWith method");
        fruits.mergeWith(moreFruits) // work both Flux parallel but order is not guarantee
                        .subscribe(System.out::println);

        System.out.println();
        System.out.println("using zip method");
        Flux.zip(fruits, colors, (f, c) -> f + " is " + c) // Pairing Elements. Stops when the shortest stream ends — very useful when joining multiple service calls.
                        .subscribe(System.out::println);

//        fruits
//                .filter(fruit -> fruit.length() > 5)
//                .subscribe(
//                        (data) -> log.info("Data: {}", data)
//                );

        fruits
//                .skip(2) // skip first 2 items
//                .take(3) // takes only first 3 data from producer
//                .count() // Operator waits for the stream to complete (onComplete) before emitting the final coun. Give 3 bcz already skip 2 items. it return mono<T> for that reason give only one value
                .sort(Comparator.reverseOrder()) // sort() needs to collect all elements in memory before sorting. so for that this is not recommended way to use in production level bcz producer -> collect data -> sort -> consumer
                .subscribe(
                    (data) -> log.info("Data: {}", data)
                );

        fruits.groupBy(fruit -> fruit.length()) // group them on the basis of length
                .flatMap(group -> group.collectList()
                        .map(list -> group.key() + " -> " + list)) // key is the length count and value is actual list of the length
                .subscribe(System.out::println);


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
