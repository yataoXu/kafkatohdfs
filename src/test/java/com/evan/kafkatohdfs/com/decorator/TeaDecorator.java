package com.evan.kafkatohdfs.com.decorator;

/**
 * @Description
 * @ClassName TeaDecorator
 * @Author Evan
 * @date 2019.10.24 11:25
 */
public class TeaDecorator extends DrinkDecorator {
    public TeaDecorator(DrinkComponent drinkComponent) {
        super(drinkComponent);
    }

    @Override
    public void operation() {

        drinkComponent.operation();
        System.out.println(",with tea");
    }
}
