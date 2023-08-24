/**
 * Copyright 2023, Richard Rodgers
 * SPDX-Licence-Identifier: Apache-2.0
 */
package org.modrepo.packr;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.temporal.ChronoUnit;
import java.util.Scanner;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.nio.file.StandardCopyOption.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import static org.modrepo.packr.Bag.*;
import static org.modrepo.packr.Bag.MetadataName.*;
import static org.modrepo.packr.Serde.*;

/*
 * Basic unit tests for BagIt Library. Incomplete.
 */

@RunWith(JUnit4.class)
public class BagTest {

    public Path payload1, payload2, tag1, tag2;

    @Rule
    public final TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void createTestData() throws IOException {
         payload1 = tempFolder.newFile("payload1").toPath();
         // copy some random bits
         OutputStream out = Files.newOutputStream(payload1);
         for (int i = 0; i < 1000; i++) {
             out.write("lskdflsfevmep".getBytes());
         }
         out.close();
         // copy to all other test files
         payload2 = tempFolder.newFile("payload2").toPath();
         Files.copy(payload1, payload2, REPLACE_EXISTING);
         tag1 = tempFolder.newFile("tag1").toPath();
         Files.copy(payload1, tag1, REPLACE_EXISTING);
         tag2 = tempFolder.newFile("tag2").toPath();
         Files.copy(payload1, tag2, REPLACE_EXISTING);
    }

    // tests for payload references (fetch.txt)
    @Test
    public void partsPresentFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag1").toPath();
        URI location = new URI("http://www.example.com/foo");
        var bag = new BagBuilder(bagFile).payload("first.pdf", payload1).payloadRef("second/second.pdf", payload2, location).build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        assertTrue(!bag.isComplete());
    }

    @Test(expected = IOException.class)
    public void nonAbsoluteURIFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag2").toPath();
        URI location = new URI("/www.example.com/foo");
        var bag = new BagBuilder(bagFile).payload("first.pdf", payload1).payloadRef("second/second.pdf", payload2, location).build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        assertTrue(!bag.isComplete());
    }

    @Test
    public void streamReadFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag3").toPath();
        URI location = new URI("http://www.example.com/foo");
        InputStream plIS = Files.newInputStream(payload1);
        var bag = new BagBuilder(bagFile).payloadRef("second/second.pdf", plIS, location).build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        assertTrue(!bag.isComplete());
    }

    @Test
    public void partsPresentUnsafeFetchBag() throws IOException, URISyntaxException {
        Path bagFile = tempFolder.newFolder("ft-bag4").toPath();
        URI location = new URI("http://www.example.com/foo");
        Map<String, String> algs = Map.of("SHA-512", "0f9d6b52621011d46dabe200fd28ab35f48665e2de4c728ff1b26178d746419df3f43d51057337362f2ab987d8dbdffd8df1ff91c4d65777d77dea38b48cb4dd");
        var bag = new BagBuilder(bagFile).payload("first.pdf", payload1).payloadRefUnsafe("second/second.pdf", 9070L, location, algs).build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(!Files.exists(pload2));
        Path fetch = bagFile.resolve("fetch.txt");
        assertTrue(Files.exists(fetch));
         // assure incompleteness
        assertTrue(!bag.isComplete());
    }

    @Test
    public void multiPayloadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag3").toPath();
        var bag = new BagBuilder(bagFile).payload("first.pdf", payload1).payload("second/second.pdf", payload2).build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path pload2 = payloadDir.resolve("second/second.pdf");
        assertTrue(Files.exists(pload2));
         // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void multiTagBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag4").toPath();
        var bag = new BagBuilder(bagFile).tag("first.pdf", tag1).tag("second/second.pdf", tag2).build();
        Path tagDir = bagFile.resolve("second");
        assertTrue(Files.isDirectory(tagDir));
        Path ttag1 = bagFile.resolve("first.pdf");
        assertTrue(Files.exists(ttag1));
        Path ttag2 = tagDir.resolve("second.pdf");
        assertTrue(Files.exists(ttag2));
         // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void metadataBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag5").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        String val1 = "metadata value";
        String val2 = "JUnit4 Test Harness";
        filler.metadata("Metadata-test", val1);
        filler.metadata(SOURCE_ORG, val2);
        Bag bag = filler.build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        assertTrue(Files.isDirectory(payloadDir));
        Path payload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(payload1));
        assertTrue(bag.metadata("Metadata-test").get(0).equals(val1));
        assertTrue(bag.metadata(SOURCE_ORG).get(0).equals(val2));
    }

    @Test
    public void autoGenMetadataBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag6").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        String val1 = "metadata value";
        String val2 = "JUnit4 Test Harness";
        filler.metadata("Metadata-test", val1);
        filler.metadata(SOURCE_ORG, val2);
        Bag bag = filler.build();
        Path payloadDir = bagFile.resolve("data");
        assertTrue(Files.isDirectory(payloadDir));
        Path payload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(payload1));
        assertNotNull(bag.metadata(BAGGING_DATE));
        assertNotNull(bag.metadata(BAG_SIZE));
        assertNotNull(bag.metadata(PAYLOAD_OXUM));
        assertNotNull(bag.metadata(BAG_SOFTWARE_AGENT));
        Path bagFile2 = tempFolder.newFolder("bag7").toPath();
        BagBuilder filler2 = new BagBuilder(bagFile2).payload("first.pdf", payload1);
        filler2.autoGenerate(new HashSet<>()).metadata(SOURCE_ORG, val2);
        Bag bag2 =  filler2.build();
        assertTrue(bag2.metadata(BAGGING_DATE).isEmpty());
        assertTrue(bag2.metadata(BAG_SIZE).isEmpty());
        assertTrue(bag2.metadata(PAYLOAD_OXUM).isEmpty());
        Path bagFile3 = tempFolder.newFolder("bag7a").toPath();
        BagBuilder filler3 = new BagBuilder(bagFile3).payload("first.pdf", payload1);
        Set<Bag.MetadataName> names = new HashSet<>();
        names.add(BAG_SIZE);
        names.add(PAYLOAD_OXUM);
        filler3.autoGenerate(names);
        Bag bag3 = filler3.build();
        assertTrue(bag3.metadata(BAGGING_DATE).isEmpty());
        assertNotNull(bag3.metadata(BAG_SIZE));
        assertNotNull(bag3.metadata(PAYLOAD_OXUM));
    }

    @Test
    public void unknownMetadataName() throws IOException {
        Path bagFile = tempFolder.newFolder("bag6a").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        String val1 = "metadata value";
        String val2 = "JUnit5 Test Harness";
        filler.metadata("Metadata-test", val1);
        filler.metadata(SOURCE_ORG, val2);
        Bag bag = filler.build();
        assertTrue(bag.metadata(SOURCE_ORG).size() > 0);
        assertTrue(bag.metadata("foobar").isEmpty());
        assertTrue(bag.property("nowhere", "foobar").isEmpty());
    }

    @Test
    public void completeAndIncompleteBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag8").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1).payload("second.pdf", payload2);
        Bag bag = filler.build();
        assertTrue(bag.isComplete());
        // now remove a payload file
        Path toDel = bagFile.resolve("data/first.pdf");
        Files.delete(toDel);
        assertTrue(!bag.isComplete());
    }

    @Test
    public void validAndInvalidBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag9").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        Bag bag = filler.build();
        assertTrue(bag.isValid());
        // now remove a payload file
        Path toDel = bagFile.resolve("data/first.pdf");
        Files.delete(toDel);
        assertTrue(!bag.isValid());
    }

    @Test
    public void bagPackandLoadRoundtrip() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag10").toPath();
        Path pkgDir = tempFolder.newFolder("bag10p").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        Path bagPackage = toPackage(filler.build(), Optional.of(pkgDir));
        Bag bag = fromPackage(bagPackage, false, Optional.empty());
        Path payload = bag.payloadFile("first.pdf");
        assertTrue(Files.size(payload1) == Files.size(payload));
    }

    @Test
    public void bagFileAttributesPreservedInZip() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag11").toPath();
        BasicFileAttributes beforeAttrs = Files.readAttributes(payload1, BasicFileAttributes.class);
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        // should preserve file time attrs if noTime false
        Path bagPackage = toPackage(filler.build(), "zip", false, Optional.empty());
        Bag bag = fromPackage(bagPackage, false, Optional.empty());
        Path payload = bag.payloadFile("first.pdf");
        BasicFileAttributes afterAttrs = Files.readAttributes(payload, BasicFileAttributes.class);
        // zip packages seem to lose millisecond precision in attributes, will agree in seconds
        assertTrue(beforeAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
        assertTrue(beforeAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
    }

    @Test
    public void bagFileAttributesPreservedInTGZ() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag11b").toPath();
        BasicFileAttributes beforeAttrs = Files.readAttributes(payload1, BasicFileAttributes.class);
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        // should preserve file time attrs if noTime false
        Path bagPackage = toPackage(filler.build(), "tgz", false, Optional.empty());
        Bag bag = fromPackage(bagPackage, false, Optional.empty());
        Path payload = bag.payloadFile("first.pdf");
        BasicFileAttributes afterAttrs = Files.readAttributes(payload, BasicFileAttributes.class);
        // zip packages seem to lose millisecond precision in attributes, will agree in seconds
        assertTrue(beforeAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.creationTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
        assertTrue(beforeAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)
        .compareTo(afterAttrs.lastModifiedTime().toInstant().truncatedTo(ChronoUnit.SECONDS)) == 0);
    }

    @Test
    public void alternateArchiveNameTGZ() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag11c").toPath();
        Path altFile = tempFolder.newFolder("bag11c.tar.gz").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        Path bagPackage = toPackage(filler.build(), "tgz", false, Optional.empty());
        // copy to alternate form .tgz => .tar.gz
        Files.move(bagPackage, altFile, StandardCopyOption.REPLACE_EXISTING);
        Bag bag = fromPackage(altFile, false, Optional.empty());
        Path payload = bag.payloadFile("first.pdf");
        assertTrue(Files.size(payload1) == Files.size(payload));
    }

    @Test
    public void bagFileAttributesClearedInZipNt() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag12").toPath();
        BasicFileAttributes beforeAttrs = Files.readAttributes(payload1, BasicFileAttributes.class);
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        // should strip file time attrs if noTime true
        Path bagPackage = toPackage(filler.build(), "zip", true, Optional.empty());
        Bag bag = fromPackage(bagPackage, false, Optional.empty());
        Path payload = bag.payloadFile("first.pdf");
        BasicFileAttributes afterAttrs = Files.readAttributes(payload, BasicFileAttributes.class);
        assertTrue(beforeAttrs.creationTime().compareTo(afterAttrs.creationTime()) != 0);
        assertTrue(beforeAttrs.lastModifiedTime().compareTo(afterAttrs.lastModifiedTime()) != 0);
    }

    @Test(expected = IllegalAccessException.class)
    public void opaqueBagAccess() throws IOException, IllegalAccessException {
        Path bagFile = tempFolder.newFolder("bag13").toPath();
        BagBuilder filler = new BagBuilder(bagFile).payload("first.pdf", payload1);
        Path bagPackage = toPackage(filler.build(), Optional.empty());
        Bag bag = fromPackage(bagPackage, true, Optional.empty());
        // stream access OK
        assertNotNull(bag.payloadStream("first.pdf"));
        // will throw IllegalAccessException
        Path payload = bag.payloadFile("first.pdf");
    }

    @Test
    public void streamReadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag14").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        InputStream plIS = Files.newInputStream(payload1);
        InputStream tagIS = Files.newInputStream(tag1);
        Bag bag = filler.payload("first.pdf", plIS).tag("firstTag.txt", tagIS).build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path ttag1 = bagFile.resolve("firstTag.txt");
        assertTrue(Files.exists(ttag1));
        // assure completeness
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void streamWrittenPayloadBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag15").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        plout.close();
        filler.build();
        // write the same to dup file
        Path dupFile = tempFolder.newFile("dupFile").toPath();
        OutputStream dupOut = Files.newOutputStream(dupFile);
        for (int i = 0; i < 1000; i++) {
            dupOut.write("lskdflsfevmep".getBytes());
        }
        dupOut.close();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        assertTrue(Files.size(pload1) == Files.size(dupFile));
         // assure completeness
        Bag bag = fromDirectory(bagFile, false);
        try {
            Path pload2 = bag.payloadFile("first.pdf");
            assertTrue(Files.size(pload2) == Files.size(dupFile));
        } catch (Exception e) {}
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test
    public void streamFilePayloadParityBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag16").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        filler.payload("first.pdf", payload1);
        OutputStream dupOut = filler.payloadStream("second.pdf");
        // write the same as stream
        InputStream dupIn = Files.newInputStream(payload1);
        int read = dupIn.read();
        while(read != -1) {
            dupOut.write(read);
            read = dupIn.read();
        }
        dupIn.close();
        dupOut.close();
        Bag fullBag = filler.build();
        try {
            assertTrue(Files.size(fullBag.payloadFile("first.pdf")) == Files.size(fullBag.payloadFile("second.pdf")));
        } catch (Exception e) {}
        Map<String, String> manif = fullBag.payloadManifest("SHA-512");
        assertTrue(manif.get("data/first.pdf").equals(manif.get("data/second.pdf")));
    }

    @Test
    public void streamWrittenBag() throws IOException {
        Path bagFile = tempFolder.newFolder("bag17").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        plout.close();
        OutputStream rout = filler.tagStream("rootTag.txt");
        for (int i = 0; i < 1000; i++) {
            rout.write("lskdflsfevmep".getBytes());
        }
        rout.close();
        OutputStream tout = filler.tagStream("tags/firstTag.txt");
        for (int i = 0; i < 1000; i++) {
            tout.write("lskdflsfevmep".getBytes());
        }
        tout.close();
        OutputStream dout = filler.tagStream("tags1/tags2/firstTag.txt");
        for (int i = 0; i < 1000; i++) {
            dout.write("lskdflsfevmep".getBytes());
        }
        dout.close();
        filler.build();
        Path payloadDir = bagFile.resolve(DATA_DIR);
        Path pload1 = payloadDir.resolve("first.pdf");
        assertTrue(Files.exists(pload1));
        Path rtag1 = bagFile.resolve("rootTag.txt");
        assertTrue(Files.exists(rtag1));
        Path ttag1 = bagFile.resolve("tags/firstTag.txt");
        assertTrue(Files.exists(ttag1));
        Path ttag2 = bagFile.resolve("tags1/tags2/firstTag.txt");
        assertTrue(Files.isDirectory(bagFile.resolve("tags1/tags2"), LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.exists(ttag2));
        // assure completeness
        Bag bag = fromDirectory(bagFile, false);
        assertTrue(bag.isComplete());
        assertTrue(bag.isValid());
    }

    @Test(expected = IOException.class)
    public void nonRenentrantFiller() throws IOException {
        // create transient filler
        BagBuilder filler = new BagBuilder().payload("first.pdf", payload1);
        InputStream in1 = toStream(filler.build());
        in1.close();
        // should throw exception here - closing in1 should delete bag
        InputStream in2 = toStream(filler.build());
        Path bagFile = tempFolder.newFolder("bag18").toPath();
        // should never reach this assert
        assertTrue(Files.notExists(bagFile));
    }

    @Test
    public void correctManifestSize() throws IOException {
        Path bagFile = tempFolder.newFolder("bag19").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        filler.payload("second.pdf", payload1);
        Bag fullBag = filler.build();
        // manifest should have 2 lines - one for each payload
        Path manif = bagFile.resolve("manifest-sha512.txt");
        assertTrue(lineCount(manif) == 2);
    }

    @Test
    public void correctManifestAPISize() throws IOException {
        Path bagFile = tempFolder.newFolder("bag20").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        filler.payload("second.pdf", payload1);
        // without serialization, only non-stream payload available in manifest count
        assertTrue(filler.getManifest("SHA-512").size() == 1);
        // serialization required to flush payloadstream
        Bag fullBag = filler.build();
        // manifest should have 2 lines - one for each payload
        assertTrue(filler.getManifest("SHA-512").size() == 2);
    }

    @Test
    public void correctTagManifestSize() throws IOException {
        Path bagFile = tempFolder.newFolder("bag21").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        filler.payload("second.pdf", payload1);
        Bag fullBag = filler.build();
        Path manif = bagFile.resolve("tagmanifest-sha512.txt");
        // should have a line for bagit.txt, bag-info.txt, and manifest*.txt
        assertTrue(lineCount(manif) == 3);
    }

    @Test
    public void streamCloseIndifferentManifest() throws IOException {
        Path bagFile = tempFolder.newFolder("bag22").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Bag fullBag = filler.build();
        // manifest should have 2 lines - one for each payload
        Path manif = bagFile.resolve("manifest-sha512.txt");
        assertTrue(lineCount(manif) == 2);
    }

    @Test
    public void loadedFromFileComplete() throws IOException {
        Path bagFile = tempFolder.newFolder("bag23").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = toPackage(filler.build(), Optional.empty());
        // Load this bag from package file
        Bag loadedBag = fromPackage(fullBag, false, Optional.empty());
        assertTrue(loadedBag.isComplete());
    }

    @Test
    public void loadedFromFileCSANotNull() throws IOException {
        Path bagFile = tempFolder.newFolder("bag24").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = toPackage(filler.build(), Optional.empty());
        // Load this bag from package file
        Bag loadedBag = fromPackage(fullBag, false, Optional.empty());
        assertTrue(loadedBag.csAlgorithms().size() > 0);
    }

    @Test
    public void loadedFromFileExpectedCSA() throws IOException {
        Path bagFile = tempFolder.newFolder("bag25").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = toPackage(filler.build(), Optional.empty());
        // Load this bag from package file
        Bag loadedBag = fromPackage(fullBag, false, Optional.empty());
        assertTrue(loadedBag.csAlgorithms().contains("SHA-512"));
    }

    @Test
    public void loadedFromFileValid() throws IOException {
        Path bagFile = tempFolder.newFolder("bag26").toPath();
        //Path pkgFile = tempFolder.newFolder("bag26p").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = toPackage(filler.build(), Optional.empty());
        // Load this bag from package file
        Bag loadedBag = fromPackage(fullBag);
        assertTrue(loadedBag.isValid());
    }

    @Test
    public void loadedFromDirectoryValid() throws IOException {
        Path bagFile = tempFolder.newFolder("bag27").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Bag bag = filler.build();
        // Load this bag from package file
        assertTrue(bag.isValid());
    }

    @Test
    public void loadedFromStreamComplete() throws IOException {
        Path bagFile = tempFolder.newFolder("bag28").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = toPackage(filler.build(), Optional.empty());
        // Load this bag from stream
        Bag loadedBag = fromStream(Files.newInputStream(fullBag), "zip", false, Optional.empty());
        assertNotNull(loadedBag.payloadManifest("SHA-512"));
        assertTrue(loadedBag.isComplete());
    }

    @Test
    public void loadedFromStreamToDirComplete() throws IOException {
        Path bagFile = tempFolder.newFolder("bag29").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = toPackage(filler.build(), Optional.empty());
        // Load this bag from stream
        Path newBag = tempFolder.newFolder("bag30").toPath();
        Bag loadedBag = fromStream(Files.newInputStream(fullBag), "zip", false, Optional.of(newBag));
        assertTrue(loadedBag.isComplete());
    }

    @Test
    public void loadedFromStreamValid() throws IOException {
        Path bagFile = tempFolder.newFolder("bag31").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        // note - client is closing stream
        plout.close();
        filler.payload("second.pdf", payload1);
        Path fullBag = toPackage(filler.build(), Optional.empty());
        // Load this bag from stream
        Bag loadedBag = fromStream(Files.newInputStream(fullBag), "zip", false, Optional.empty());
        assertTrue(loadedBag.isValid());
    }

    @Test
    public void defaultEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag32").toPath();
        BagBuilder filler = new BagBuilder(bagFile);
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Bag fullBag = filler.build();
        // use bag-info.txt as representative text-file
        Path info = bagFile.resolve("bag-info.txt");
        // line ending is same as system-defined one
        assertTrue(fullBag.isValid());
        assertTrue(findSeparator(info).equals(System.lineSeparator()));
    }

    @Test
    public void unixEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag33").toPath();
        BagBuilder filler = new BagBuilder(bagFile, StandardCharsets.UTF_8, BagBuilder.EolRule.UNIX, false, "SHA-512");
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Bag fullBag = filler.build();
        // use bag-info.txt as representative text-file
        Path info = bagFile.resolve("bag-info.txt");
        // line ending is same as system-defined one
        assertTrue(fullBag.isValid());
        assertTrue(findSeparator(info).equals("\n"));
    }

    @Test
    public void windowsEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag34").toPath();
        BagBuilder filler = new BagBuilder(bagFile, StandardCharsets.UTF_8, BagBuilder.EolRule.WINDOWS, false, "SHA-512");
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Bag fullBag = filler.build();
        // use bag-info.txt as representative text-file
        Path info = bagFile.resolve("bag-info.txt");
        // line ending is same as system-defined one
        assertTrue(fullBag.isValid());
        assertTrue(findSeparator(info).equals("\r\n"));
    }

    @Test
    public void counterEOLInTextFiles() throws IOException {
        Path bagFile = tempFolder.newFolder("bag35").toPath();
        BagBuilder filler = new BagBuilder(bagFile, StandardCharsets.UTF_8, BagBuilder.EolRule.COUNTER_SYSTEM, false, "SHA-512");
        OutputStream plout = filler.payloadStream("first.pdf");
        for (int i = 0; i < 1000; i++) {
            plout.write("lskdflsfevmep".getBytes());
        }
        Bag fullBag = filler.build();
        // use bag-info.txt as representative text-file
        Path info = bagFile.resolve("bag-info.txt");
        // line ending is not the same as system-defined one
        assertTrue(fullBag.isValid());
        assertTrue(! findSeparator(info).equals(System.lineSeparator()));
    }

    @Test
    public void validAndInvalidBagUTF16() throws IOException {
        Path bagFile = tempFolder.newFolder("bag36").toPath();
        BagBuilder filler = new BagBuilder(bagFile, StandardCharsets.UTF_16, "MD5").payload("first.pdf", payload1);
        Bag bag = filler.build();
        Map<String, String> tman = bag.tagManifest("MD5");
        assertTrue(tman.size() == 3);
        assertTrue(tman.keySet().contains("bagit.txt"));
        assertTrue(tman.keySet().contains("bag-info.txt"));
        assertTrue(tman.keySet().contains("manifest-md5.txt"));
        assertTrue(bag.isValid());
        // now remove a payload file
        Path toDel = bagFile.resolve("data/first.pdf");
        Files.delete(toDel);
        assertTrue(!bag.isValid());
    }

    private String findSeparator(Path file) throws IOException {
        try (Scanner scanner = new Scanner(file)) {
            // it's one or the other
            return (scanner.findWithinHorizon("\r\n", 500) != null) ? "\r\n" : "\n";
        }
    }

    private int lineCount(Path file) throws IOException {
        Scanner scanner = new Scanner(file);
        int count = 0;
        while (scanner.hasNext()) {
            count++;
            scanner.nextLine();
        }
        scanner.close();
        return count;
    }
}
