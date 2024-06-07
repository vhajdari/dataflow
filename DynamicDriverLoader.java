public class DynamicDriverLoader {

    public static Class<?> loadDriver(String jarPath) throws ClassNotFoundException, IOException {
        File jarFile = new File(jarPath);
        URL url = jarFile.toURI().toURL();
        URLClassLoader classLoader = new URLClassLoader(new URL[]{url});
        
        // Iterate through loaded classes to find the driver class
        for (Class<?> loadedClass : classLoader.getLoadedClasses()) {
            if (java.sql.Driver.class.isAssignableFrom(loadedClass)) {
                return loadedClass;
            }
        }
        throw new ClassNotFoundException("JDBC Driver class not found in JAR: " + jarPath);
    }
}
