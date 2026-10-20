package com.campusmq.broker.store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;

/** 测试前置清理：删除上次运行残留的存储目录。仅用于测试类。 */
public final class TestStoreCleaner {

    private TestStoreCleaner() {
    }

    public static void clean(String relativeDir) {
        Path dir = Paths.get(relativeDir);
        if (!Files.exists(dir)) {
            return;
        }
        try (var stream = Files.walk(dir)) {
            stream.sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            throw new RuntimeException("delete failed: " + p, e);
                        }
                    });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
