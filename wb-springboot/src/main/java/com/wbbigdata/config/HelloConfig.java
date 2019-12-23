package com.wbbigdata.config;

import com.wbbigdata.service.HelloService;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @Configuration 指明当前类是一个配置类
 */
@Configuration
public class HelloConfig {

    // 将该方法的返回值注入到容器中，容器中这个组件默认的id就是方法名
    @Bean
    public HelloService helloService(){
        return new HelloService();
    }
}
