package com.evan.kafkatohdfs.com.decorator;

/**
 * @Description
 * @ClassName SugarDecorator
 * @Author Evan
 * @date 2019.10.24 11:23
 */
public class SugarDecorator extends DrinkDecorator {

    public SugarDecorator(DrinkComponent drinkComponent) {
        super(drinkComponent);
    }

    @Override
    public void operation() {
        drinkComponent.operation();
        System.out.println(",with sugar");
    }
}
