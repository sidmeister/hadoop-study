package com.sid.hadoop;

    import java.io.File;
    import java.io.FileInputStream;
    import java.io.IOException;
    import java.util.Properties;

    /**
     * This class is responsible for loading the application level properties from
     * the app.properties
     */
    public class AppProperties
    {
        public static final String APP_PROPERTIES_FILE_NAME = "app.properties";

        public static final String EXTERNAL_APP_PROPERTIES = "EXTERNAL_APP_PROPERTIES";

        public Properties properties;

        /**
         *
         * @return The application properties stored in key value pairs.
         * @throws IOException
         *             when we cannot find the app.properties file
         */
        public Properties readProperties() throws IOException
        {
            properties = new Properties();
            properties.load(this.getClass().getClassLoader().getResourceAsStream(APP_PROPERTIES_FILE_NAME));

            if (properties.containsKey(EXTERNAL_APP_PROPERTIES)) {
                String filePathStr = (String) properties.get(EXTERNAL_APP_PROPERTIES);
                File externalAppPropertiesFile = new File(filePathStr);
                if (externalAppPropertiesFile.exists() && externalAppPropertiesFile.canRead()) {
                    properties.load(new FileInputStream(externalAppPropertiesFile));
                }
            }
            return properties;
        }
    }
