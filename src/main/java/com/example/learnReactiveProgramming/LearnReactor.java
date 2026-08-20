package com.example.learnReactiveProgramming;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Component;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

@Component
@Slf4j
public class LearnReactor {
//    Producer
    public void learReactor(){
        Flux<String> fruits = Flux.just("apple", "litchi", "Banana")
                        .map(fruit->{
                           if (fruit.equals("Banana"))
                               throw new RuntimeException("No likely Banana");

                            return fruit;
                        });

        fruits.subscribe(new BaseSubscriber<String>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                log.info("hookOnSubscribe");
                request(1);
            }

            @Override
            protected void hookOnNext(String value) {
                log.info("Processing: {}", value);
                if(value.equals("litchi")) cancel();
                request(1);
            }

            @Override
            protected void hookOnComplete() {
                log.info("hookOnComplete");
            }

            @Override
            protected void hookOnError(Throwable throwable) {
                log.info("hookOnError : {}", throwable.getMessage(), throwable);
            }

            @Override
            protected void hookOnCancel() {
                log.info("hookOnCancel");
            }

            @Override
            protected void hookFinally(SignalType type){
                log.info("hookOnFinally {}", type.name());
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
