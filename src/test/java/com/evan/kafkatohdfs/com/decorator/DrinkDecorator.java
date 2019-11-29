package com.evan.kafkatohdfs.com.decorator;

/**
 * @Description
 * @ClassName DrinkDecorator
 * @Author Evan
 * @date 2019.10.24 11:21
 */
public class DrinkDecorator implements DrinkComponent {

    DrinkComponent drinkComponent;

    public DrinkDecorator(DrinkComponent drinkComponent){
        super();
        this.drinkComponent = drinkComponent;
    }
    @Override
    public void operation() {

    }
}
