package com.example.learnReactiveProgramming;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Component;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

@Component
@Slf4j
public class LearnReactor {
//    Producer
    public void learReactor(){
        Flux<String> fruits = Flux.just("apple", "litchi", "Banana")
                        .map(fruit->{
                           if (fruit.equals("litchi"))
                               throw new RuntimeException("No likely litchi");

                            return fruit;
                        });

        fruits.subscribe(new BaseSubscriber<String>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                super.hookOnSubscribe(subscription);
            }

            @Override
            protected void hookOnNext(String value) {
                super.hookOnNext(value);
            }

            @Override
            protected void hookOnComplete() {
                super.hookOnComplete();
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                super.hookOnError(throwable);
            }

            @Override
            protected void hookOnCancel() {
                super.hookOnCancel();
            }
        });


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
