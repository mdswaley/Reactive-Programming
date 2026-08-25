package com.example.learnReactiveProgramming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class LearnError {
    public void learnErrorHandling(){
        Mono<String> fromCallable = Mono.fromCallable(()->{
            Thread.sleep(4000);
            throw new RuntimeException("error occur");
        });

        fromCallable.subscribe(data -> log.info("data : {}",data));
    }

}
