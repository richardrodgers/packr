/**
 * Copyright 2023, Richard Rodgers
 * SPDX-Licence-Identifier: Apache-2.0
 */
package org.modrepo.packr;

import java.io.OutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.file.StandardCopyOption.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.jupiter.api.Assertions.*;

import static org.modrepo.packr.Bag.*;

/*
 * Basic unit tests for BagIt Library. Incomplete.
 */

public class BagChecksum {

    public static Path payload1, payload2, tag1, tag2;

    @TempDir
    public static File tempFolder;

    @BeforeAll
    public static void createTestData() throws IOException {
         payload1 = new File(tempFolder, "payload1").toPath();
         // copy some random bits
         OutputStream out = Files.newOutputStream(payload1);
         for (int i = 0; i < 1000; i++) {
             out.write("lskdflsfevmep".getBytes());
         }
         out.close();
         // copy to all other test files
         payload2 = new File(tempFolder, "payload2").toPath();
         Files.copy(payload1, payload2, REPLACE_EXISTING);
         tag1 = new File(tempFolder, "tag1").toPath();
         Files.copy(payload1, tag1, REPLACE_EXISTING);
         tag2 = new File(tempFolder, "tag2").toPath();
         Files.copy(payload1, tag2, REPLACE_EXISTING);
    }

    // tests for various checksum related functions

    @Test
    void basicBagPartsPresentDefaultChecksum(@TempDir Path bagFile) throws IOException, IllegalAccessException {
        var bag = new BagBuilder(bagFile).payload(payload1).build();
        // verify all required files present
        assertTrue(Files.exists(bagFile.resolve(DECL_FILE)));
        // should default to SHA-512
        assertTrue(Files.exists(bagFile.resolve("manifest-sha512.txt")));
        assertTrue(Files.exists(bagFile.resolve("tagmanifest-sha512.txt")));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    void basicBagPartsPresentMD5(@TempDir Path bagFile) throws IOException, IllegalAccessException {
        var bag = new BagBuilder(bagFile, "MD5").payload(payload1).build();;
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest = bagFile.resolve("manifest-md5.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-md5.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void basicBagPartsPresentSHA1(@TempDir Path bagFile) throws IOException, IllegalAccessException {
        var bag = new BagBuilder(bagFile, "SHA-1").payload(payload1).build();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest = bagFile.resolve("manifest-sha1.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-sha1.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void basicBagPartsPresentSHA256(@TempDir Path bagFile) throws IOException, IllegalAccessException {
        var bag = new BagBuilder(bagFile, "SHA-256").payload(payload1).build();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest = bagFile.resolve("manifest-sha256.txt");
        assertTrue(Files.exists(manifest));
        Path tagmanifest = bagFile.resolve("tagmanifest-sha256.txt");
        assertTrue(Files.exists(tagmanifest));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void unknownChecksumAlgorithm(@TempDir Path bagFile) throws /*IOException, */ IllegalAccessException {
        IOException ioE = assertThrows(IOException.class, () -> {
            new BagBuilder(bagFile, "SHA-6666");
            },
            "Should throw exception when checksum algorithm unknown"
        );
        assertTrue(ioE.getClass().equals(IOException.class));
    }

    @Test
    public void basicBagPartsPresentMultiChecksum(@TempDir Path bagFile) throws IOException, IllegalAccessException {
        var bag = new BagBuilder(bagFile, "SHA-256", "SHA-512").payload(payload1).build();
        Path decl = bagFile.resolve(DECL_FILE);
        assertTrue(Files.exists(decl));
        Path manifest1 = bagFile.resolve("manifest-sha256.txt");
        assertTrue(Files.exists(manifest1));
        Path manifest2 = bagFile.resolve("manifest-sha512.txt");
        assertTrue(Files.exists(manifest2));
        Path tagmanifest1 = bagFile.resolve("tagmanifest-sha256.txt");
        assertTrue(Files.exists(tagmanifest1));
        Path tagmanifest2 = bagFile.resolve("tagmanifest-sha512.txt");
        assertTrue(Files.exists(tagmanifest2));
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payloadFile = payloadDir.resolve(payload1.getFileName().toString());
        assertTrue(Files.exists(payloadFile));
        // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }
}
