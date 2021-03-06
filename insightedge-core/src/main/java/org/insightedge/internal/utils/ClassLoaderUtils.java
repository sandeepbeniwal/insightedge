/*
 * Copyright (c) 2019, GigaSpaces Technologies, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.insightedge.internal.utils;

import com.gigaspaces.api.InternalApi;
import com.gigaspaces.start.ClasspathBuilder;
import com.gigaspaces.start.SystemLocations;
import org.jini.rio.boot.ServiceClassLoader;

import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.function.Predicate;

/**
 * @author Niv Ingberg
 * @since 14.2
 */
@InternalApi
public class ClassLoaderUtils {
    public static ClasspathBuilder getSparkClassPath() {
        return getSparkClassPath(ClassLoaderUtils::sparkJarsFilter);
    }

    public static ClasspathBuilder getSparkClassPath(Predicate<Path> sparkJarsFilter) {
        return new ClasspathBuilder()
                //.appendPlatformJars("scala")
                .appendJars(SystemLocations.singleton().sparkHome().resolve("jars"), sparkJarsFilter);
    }

    private static boolean sparkJarsFilter(Path path) {
        String jarName = path.getFileName().toString();
        return  !jarName.startsWith("xerces");
    }
}
