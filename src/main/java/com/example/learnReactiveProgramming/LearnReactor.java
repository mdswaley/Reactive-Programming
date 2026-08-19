package com.example.learnReactiveProgramming;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
@Slf4j
public class LearnReactor {
//    Producer
    public void learReactor(){
        Flux<String> fruits = Flux.just("apple", "litchi", "Banana");

        fruits.subscribe(
                new Subscriber<>() {
                    @Override
                    public void onSubscribe(Subscription subscription) {
                        log.info("onSubscribe");
                        subscription.request(1);
                    }

                    @Override
                    public void onNext(String s) {
                        log.info("Fruit: {}", s);
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        log.error("onError {}", throwable.getMessage());
                    }

                    @Override
                    public void onComplete() {
                        log.info("onComplete");
                    }
                }
        );
    }
}
