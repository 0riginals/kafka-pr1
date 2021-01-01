package org.m2tnsi.pr1.runner;

import org.m2tnsi.pr1.producer.Producer1;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.Timer;

@Component
public class Runner implements CommandLineRunner {
    @Override
    public void run(String... args) throws Exception {

        /* -- On prévoit notre action sur l'API Covid 19 tous les 1000 ms*60 pour faire une minute et *30 pour
         faire notre planning tous les 30 minutes. -- */
        Timer timer = new Timer();
        timer.schedule(new Producer1(), 0, 1000*60*30);
    }
}

