package in.nimbo.moama.news.listener;


import java.lang.annotation.*;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)

public @interface CLI {
    String help() ;

    String name() default "";
}
