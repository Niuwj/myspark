package nwj;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BinaryOperator;

public class Java7Tester {

	public static void main(String[] args) {
		// JDK7 匿名内部类写法
		new Thread(new Runnable(){// 接口名
			@Override
			public void run(){// 方法名
				System.out.println("Thread run()");
			}
		}).start();


		// JDK8 Lambda表达式写法。 能够使用Lambda的依据是必须有相应的函数接口；Lambda表达式另一个依据是类型推断机制。
		new Thread(
				() -> System.out.println("Thread run()")// 省略接口名和方法名
		).start();

		new Thread(
				() -> {
					System.out.println("lamda Thread run()"); // 省略接口名和方法名
					System.out.println("lllll   lamda Thread run()"); // 省略接口名和方法名
				}
		).start();

		List<String> list = Arrays.asList("I", "love", "you", "too");
		Collections.sort(list, (s1, s2) ->{// 省略参数表的类型
			if(s1 == null)
				return -1;
			if(s2 == null)
				return 1;
			return s1.length()-s2.length();
		});

		// Lambda表达式的书写形式
		Runnable run = () -> System.out.println("Hello World");// 1
		Runnable multiLine = () -> {// 3 代码块
			System.out.print("Hello");
			System.out.println(" Hoolee");
		};
		BinaryOperator<Long> add = (Long x, Long y) -> x + y;// 4
		BinaryOperator<Long> addImplicit = (x, y) -> x + y;// 5 类型推断
	}
}
