package com.evan.kafkatohdfs.com.decorator;

/**
 * @Description
 * @ClassName WaterComponent
 * @Author Evan
 * @date 2019.10.24 11:20
 */
public class WaterComponent implements DrinkComponent{
    @Override
    public void operation() {
        System.out.println("water drink");
    }
}
