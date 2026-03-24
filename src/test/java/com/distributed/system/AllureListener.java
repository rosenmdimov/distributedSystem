package com.distributed.system;

import io.qameta.allure.Allure;

public class AllureListener {

    public static void log(String message) {
        Allure.step(message);
    }
}