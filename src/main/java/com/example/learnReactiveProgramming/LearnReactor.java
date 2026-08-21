package com.example.learnReactiveProgramming;

import lombok.extern.slf4j.Slf4j;

import org.springframework.stereotype.Component;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;


import java.time.Duration;

@Component
@Slf4j
public class LearnReactor {

    public void learReactor(){
        //    Producer

        Flux<String> fruits = Flux.just("apple", "litchi", "Banana")
                        .map(fruit->{ // Processor or operator
                           if (fruit.equals("Banana"))
                               throw new RuntimeException("No likely Banana");

                            return fruit;
                        });

//        Interval:- creates a Flux that emits values repeatedly at a fixed time interval
        Flux<String> flux = Flux.interval(Duration.ofSeconds(1))
                        .take(5)
                        .map(tick -> "tick : "+tick); // Processor or operator

//        Subscriber
        flux
                .doOnSubscribe(sub -> log.info("on Subscribe"))
                .doOnNext(item -> log.info("logging item {}",item))
                .subscribe(
                item -> {
                    log.info("Processing: {}", item);
                },
                err ->{
                    log.error("onError ", err);
                },
                () -> {
                    log.info("onComplete");
                }
        );

//        fruits.subscribe(new BaseSubscriber<String>() {
//            Subscription
//            @Override
//            protected void hookOnSubscribe(Subscription subscription) {
//                log.info("hookOnSubscribe");
//                request(1);
//            }
//
//            @Override
//            protected void hookOnNext(String value) {
//                log.info("Processing: {}", value);
//                if(value.equals("litchi")) cancel();
//                request(1);
//            }
//
//            @Override
//            protected void hookOnComplete() {
//                log.info("hookOnComplete");
//            }
//
//            @Override
//            protected void hookOnError(Throwable throwable) {
//                log.info("hookOnError : {}", throwable.getMessage(), throwable);
//            }
//
//            @Override
//            protected void hookOnCancel() {
//                log.info("hookOnCancel");
//            }
//
//            @Override
//            protected void hookFinally(SignalType type){
//                log.info("hookOnFinally {}", type.name());
//            }
//        });


//
//        fruits.subscribe(
//                new Subscriber<>() {
//                    Subscription subscription;
//                    @Override
//                    public void onSubscribe(Subscription s) {
//                        subscription = s;
//                        log.info("onSubscribe");
//                        s.request(1);
//                    }
//
//                    @Override
//                    public void onNext(String s) {
//                        log.info("Fruit: {}", s);
//                        subscription.request(1);
//                    }
//
//                    @Override
//                    public void onError(Throwable throwable) {
//                        log.error("onError {}", throwable.getMessage());
//                    }
//
//                    @Override
//                    public void onComplete() {
//                        log.info("onComplete");
//                    }
//                }
//        );
    }
}
