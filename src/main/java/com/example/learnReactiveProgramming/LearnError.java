package com.example.learnReactiveProgramming;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
@Slf4j
public class LearnError {
    public void learnErrorHandling(){
//        after using try-catch not able to catch the exception
//        bcz after 4s long-running stream the try-catch already gone
        try {
            Mono<String> fromCallable = Mono.fromCallable(()->{
                Thread.sleep(4000);
                throw new RuntimeException("error occur");
            });

            fromCallable.subscribe(data -> log.info("data : {}",data));
        }catch (Exception e){
            log.error("Caught: ", e);
        }

    }

}
