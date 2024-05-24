import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        System.out.println("Hello world!");

        CacheFactory<Integer, Integer> factory = new CacheFactory<>();
        Cache<Integer, Integer> cache = factory.getLRUCache(5);


        System.out.println("Hello to cache world");

        while (true) {

            Scanner scanner = new Scanner(System.in);

            // Prompt the user for input
            System.out.print("Enter something: operation \n" +
                    "1. getCache \n" +
                    "2. addToCache \n");

            // Read the input provided by the user
            String userInput = scanner.nextLine();

            if (userInput.equals("1")) {
                System.out.println("Enter key");

                Integer key = scanner.nextInt();

                try {
                    System.out.println("Cache hit "+cache.getKey(key));
                } catch (CacheNotFoundException e) {
                    System.out.println("cache not found");
                } catch (CacheIsEmpty e) {
                    System.out.println("cache is empty");
                }
            } else {
                System.out.println("Enter key");
                Integer key = scanner.nextInt();
                System.out.println("Enter value");
                Integer value = scanner.nextInt();


                try {
                    cache.addToCache(key, value);
                } catch (CacheIsEmpty e) {
                    System.out.println("cache is empty");
                }

            }
        }

    }
}