//usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.7.5
//JAVA 17

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReference;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

@Command(name = "checkVersions", mixinStandardHelpOptions = true,
         description = "Check if a Flink version is supported by the Kubernetes Operator")
class checkVersions implements Callable<Integer> {

    private static final Pattern SEMVER_PATTERN =
        Pattern.compile("^(0|[1-9]\\d*)\\.(0|[1-9]\\d*)\\.(0|[1-9]\\d*)(?:-((?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\\.(?:0|[1-9]\\d*|\\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\\+([0-9a-zA-Z-]+(?:\\.[0-9a-zA-Z-]+)*))?$");

    @Parameters(index = "0", description = "Flink version to check (semver format)", defaultValue = "")
    private String versionToCheck;

    @Option(names = {"-l", "--list"}, description = "List all supported versions")
    private boolean listVersions = false;

    @Option(names = {"-o", "--operator-version"}, description = "Flink Kubernetes Operator API version", defaultValue = "1.11.0")
    private String operatorVersion;

    public static void main(String... args) {
        int exitCode = new CommandLine(new checkVersions()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {

        // Download the operator API jar
        String apiJarPath = downloadDependency("org.apache.flink", "flink-kubernetes-operator-api", operatorVersion);
        File apiJarFile = new File(apiJarPath);

        // Create a new class loader with the jar file
        URL jarUrl = apiJarFile.toURI().toURL();
        URLClassLoader childClassLoader = new URLClassLoader(
            new URL[] {jarUrl},
            this.getClass().getClassLoader()
        );

        // Load FlinkVersion class
        Class<?> flinkVersionClass;
        try {
            flinkVersionClass = Class.forName(
                "org.apache.flink.kubernetes.operator.api.spec.FlinkVersion",
                true,
                childClassLoader
            );
        } catch (ClassNotFoundException e) {
            System.err.println("Error: Could not find FlinkVersion class in the specified operator version");
            return 1;
        }

        if (listVersions) {
            listSupportedVersions(flinkVersionClass);
            return 0;
        }

        if (versionToCheck.isEmpty()) {
            System.err.println("Error: Please provide a Flink version to check or use --list");
            return 1;
        }

        if (!isValidSemver(versionToCheck)) {
            System.err.println("Error: '" + versionToCheck + "' is not a valid semantic version");
            return 1;
        }

        VersionCheckResult versionCheckResult = checkVersionSupport(versionToCheck, flinkVersionClass);
        if (versionCheckResult.supported && !versionCheckResult.deprecated) {
            System.out.println(
                "Flink version " + versionToCheck + " is supported by the Kubernetes Operator version " + operatorVersion
            );
            return 0;
        } else if (versionCheckResult.supported && versionCheckResult.deprecated) {
            System.out.println(
                "Flink version " + versionToCheck + " is supported by the Kubernetes Operator version " + operatorVersion + " but is deprecated"
            );
            return 1;
        } else {
            System.out.println(
                "Flink version " + versionToCheck + " is not supported by the Kubernetes Operator version " + operatorVersion
            );
            return 1;
        }
    }

    /**
     * Downloads a Maven dependency to the local .jbang cache
     *
     * @param groupId Maven group ID
     * @param artifactId Maven artifact ID
     * @param version Version
     * @return Path to the downloaded JAR
     */
    private String downloadDependency(String groupId, String artifactId, String version) throws Exception {
        String jbangDir = System.getProperty("user.home") + "/.jbang";
        Path cachePath = Paths.get(jbangDir, "cache", "dependencies", groupId, artifactId, version);
        Files.createDirectories(cachePath);

        String jarName = artifactId + "-" + version + ".jar";
        Path jarPath = cachePath.resolve(jarName);

        if (!Files.exists(jarPath)) {
            System.out.println("Downloading " + groupId + ":" + artifactId + ":" + version + "...");

            // Build Maven repo URL
            String repo = "https://repo1.maven.org/maven2";
            String groupPath = groupId.replace('.', '/');
            String url = String.format("%s/%s/%s/%s/%s", repo, groupPath, artifactId, version, jarName);

            // Download the jar
            try {
                Files.copy(new URL(url).openStream(), jarPath);
            } catch (Exception e) {
                System.err.println("Failed to download: " + url);
                throw e;
            }
        }

        return jarPath.toString();
    }

    /**
     * Results of checking a Flink version against the supported versions
     */
    private static class VersionCheckResult {
        final boolean supported;
        final boolean deprecated;

        VersionCheckResult(boolean supported, boolean deprecated) {
            this.supported = supported;
            this.deprecated = deprecated;
        }
    }

    /**
     * Check if the given semver string is supported by the Flink Kubernetes Operator
     * @param version The version to check in semver format
     * @param flinkVersionClass The FlinkVersion class obtained via reflection
     * @return A VersionCheckResult containing whether the version is supported and if it's deprecated
     */
    private VersionCheckResult checkVersionSupport(String version, Class<?> flinkVersionClass) throws Exception {
        // Extract major and minor version from semver
        String[] parts = version.split("\\.");
        if (parts.length < 2) {
            return new VersionCheckResult(false, false);
        }

        int major = Integer.parseInt(parts[0]);
        int minor = Integer.parseInt(parts[1]);
        String targetEnumName = "v" + major + "_" + minor;

        // Check against available FlinkVersion enum values
        Object[] enumConstants = flinkVersionClass.getEnumConstants();
        for (Object enumConstant : enumConstants) {
            String enumName = enumConstant.toString();
            if (enumName.equals(targetEnumName)) {
                // Check if the enum value is deprecated
                for (java.lang.reflect.Field field : flinkVersionClass.getDeclaredFields()) {
                    if (field.getName().equals(targetEnumName) && field.isAnnotationPresent(Deprecated.class)) {
                        return new VersionCheckResult(true, true); // Version is supported but deprecated
                    }
                }
                return new VersionCheckResult(true, false); // Version is supported and not deprecated
            }
        }

        return new VersionCheckResult(false, false); // Version is not supported
    }

    /**
     * List all supported Flink versions from the FlinkVersion enum
     * @param flinkVersionClass The FlinkVersion class obtained via reflection
     */
    private void listSupportedVersions(Class<?> flinkVersionClass) throws Exception {
        System.out.println("Supported Flink versions by the Kubernetes Operator:");

        // Get all enum constants
        Object[] enumConstants = flinkVersionClass.getEnumConstants();
        List<Object> sortedVersions = new ArrayList<>(Arrays.asList(enumConstants));
        sortedVersions.sort(Comparator.comparing(Object::toString));

        // Check each enum constant
        for (Object version : sortedVersions) {
            String enumName = version.toString();
            String readableVersion = getReadableVersion(enumName);

            // Check if the enum value is deprecated
            boolean isDeprecated = false;
            for (java.lang.reflect.Field field : flinkVersionClass.getDeclaredFields()) {
                if (field.getName().equals(enumName) && field.isAnnotationPresent(Deprecated.class)) {
                    isDeprecated = true;
                    break;
                }
            }

            if (isDeprecated) {
                System.out.println("- " + readableVersion + " (DEPRECATED)");
            } else {
                System.out.println("- " + readableVersion);
            }
        }
    }

    /**
     * Validates if a string is a valid semantic version
     * @param version The version string to validate
     * @return true if valid semver, false otherwise
     */
    private boolean isValidSemver(String version) {
        return SEMVER_PATTERN.matcher(version).matches();
    }

    /**
     * Convert from enum format (v1_17) to readable format (1.17)
     * @param enumName The FlinkVersion enum name
     * @return A readable version string
     */
    private String getReadableVersion(String enumName) {
        return enumName.substring(1).replace("_", ".");
    }
}