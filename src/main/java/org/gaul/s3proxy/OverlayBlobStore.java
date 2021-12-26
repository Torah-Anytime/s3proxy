/*
 * Copyright 2014-2021 Andrew Gaul <andrew@gaul.org>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gaul.s3proxy;

import com.google.common.collect.ForwardingObject;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.*;
import org.jclouds.blobstore.options.*;
import org.jclouds.domain.Location;
import org.jclouds.filesystem.reference.FilesystemConstants;
import org.jclouds.io.Payload;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;



/** This class is a BlobStore wrapper which tracks write operations in the local filesystem. */
final class OverlayBlobStore extends ForwardingObject implements BlobStore {

    private static final Logger logger = LoggerFactory.getLogger(
            OverlayBlobStore.class);

    private final BlobStore filesystemBlobStore;
    private final BlobStore upstreamBlobStore;
    private final String maskSuffix;

    public OverlayBlobStore(BlobStore upstreamBlobStore, String overlayPath, String maskSuffix) {
        this.maskSuffix = maskSuffix;
        this.upstreamBlobStore = upstreamBlobStore;

        Properties properties = new Properties();
        properties.setProperty(FilesystemConstants.PROPERTY_BASEDIR, overlayPath);
        BlobStoreContext context = ContextBuilder.newBuilder("filesystem")
                .overrides(properties)
                .buildView(BlobStoreContext.class);
        filesystemBlobStore = context.getBlobStore();
    }

    protected BlobStore delegateUpstream() {
        return upstreamBlobStore;
    }

    @Override
    protected BlobStore delegate() {
        return this.filesystemBlobStore;
    }

    public BlobStore localBlobStore() {
        return this.filesystemBlobStore;
    }

    public static @NotNull BlobStore newOverlayBlobStore(BlobStore blobStore, String overlayPath, String maskSuffix) {
        return new OverlayBlobStore(blobStore, overlayPath, maskSuffix);
    }

    @Override
    public BlobStoreContext getContext() {
        return delegate().getContext();
    }

    @Override
    public BlobBuilder blobBuilder(String name) {
        return delegate().blobBuilder(name);
    }

    @Override
    public Set<? extends Location> listAssignableLocations() {
        return delegate().listAssignableLocations();
    }

    @Override
    public PageSet<? extends StorageMetadata> list() {
        PageSet<StorageMetadata> localSet = (PageSet<StorageMetadata>) delegate().list();
        PageSet<StorageMetadata> upstreamSet = (PageSet<StorageMetadata>) delegateUpstream().list();
        localSet.addAll(upstreamSet);
        return localSet;
    }

    @Override
    public boolean containerExists(String container) {
        return ensureLocalContainerExistsIfUpstreamDoes(container);
    }

    @Override
    public boolean createContainerInLocation(Location location,
                                             String container) {
        return delegate().createContainerInLocation(location, container);
    }

    @Override
    public boolean createContainerInLocation(Location location,
                                             String container, CreateContainerOptions options) {
        // TODO: Simulate error when creating a bucket that already exists
        return delegate().createContainerInLocation(location, container);
    }

    @Override
    public ContainerAccess getContainerAccess(String container) {
        return delegate().getContainerAccess(container);
    }

    @Override
    public void setContainerAccess(String container, ContainerAccess
            containerAccess) {
        delegate().setContainerAccess(container, containerAccess);
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container) {
        if ( ! ensureLocalContainerExistsIfUpstreamDoes(container) ) {
            // The upstream container doesn't exist, try to list locally to generate 404
            delegate().list(container);
        } else {
            // Upstream container exists, and local container does too. List the files from both and merge
            PageSet<StorageMetadata> localSet = (PageSet<StorageMetadata>) delegate().list(container);
            PageSet<StorageMetadata> upstreamSet = (PageSet<StorageMetadata>) delegateUpstream().list(container);
            return mergeAndFilterList(localSet, upstreamSet);
        }
        return null;
    }

    @Override
    public PageSet<? extends StorageMetadata> list(String container,
                                                   ListContainerOptions options) {

        if ( ! ensureLocalContainerExistsIfUpstreamDoes(container) ) {
            // The upstream container doesn't exist, try to list locally to generate 404
            delegate().list(container, options);
        } else {
            // Upstream container exists, and local container does too. List the files from both and merge
            PageSet<StorageMetadata> localSet = (PageSet<StorageMetadata>) delegate().list(container, options);
            PageSet<StorageMetadata> upstreamSet = (PageSet<StorageMetadata>) delegateUpstream().list(container, options);
            return mergeAndFilterList(localSet, upstreamSet);
        }
        return null;
    }

    @Override
    public void clearContainer(String container) {
        delegate().clearContainer(container);
    }

    @Override
    public void clearContainer(String container, ListContainerOptions options) {
        delegate().clearContainer(container, options);
    }

    @Override
    public void deleteContainer(String container) {
        throw new NotImplementedException();
    }

    @Override
    public boolean deleteContainerIfEmpty(String container) {
        throw new NotImplementedException();
    }

    @Override
    public boolean directoryExists(String container, String directory) {
        return delegate().directoryExists(container, directory);
    }

    @Override
    public void createDirectory(String container, String directory) {
        delegate().createDirectory(container, directory);
    }

    @Override
    public void deleteDirectory(String container, String directory) {
        delegate().deleteDirectory(container, directory);
    }

    @Override
    public boolean blobExists(String container, String name) {
        return delegate().blobExists(container, name);
    }

    @Override
    public String putBlob(String containerName, Blob blob) {
        ensureLocalContainerExistsIfUpstreamDoes(containerName);
        // TODO: Simulate error when file already exists in upstream bucket
        if(isBlobMasked(containerName, blob.getMetadata().getName())){
            unmaskBlob(containerName, blob.getMetadata().getName());
        }
        return delegate().putBlob(containerName, blob);
    }

    @Override
    public String putBlob(String containerName, Blob blob,
                          PutOptions putOptions) {
        ensureLocalContainerExistsIfUpstreamDoes(containerName);
        // TODO: Simulate error when file already exists in upstream bucket
        if(isBlobMasked(containerName, blob.getMetadata().getName())){
            unmaskBlob(containerName, blob.getMetadata().getName());
        }
        return delegate().putBlob(containerName, blob, putOptions);
    }

    @Override
    public String copyBlob(String fromContainer, String fromName, String toContainer, String toName,
                           CopyOptions options) {
        throw new NotImplementedException();
    }

    @Override
    public BlobMetadata blobMetadata(String container, String name) {
        // TODO: Find a better way to generate a 'not found error'
        if(isBlobMasked(container, name)){
            return delegate().blobMetadata(container, "oasiguaogiyhgoiayhsgdogDsfsd");
        }

        if(isBlobLocal(container, name)){
            return delegate().blobMetadata(container, name);
        } else if(delegateUpstream().blobExists(container, name)){
            return delegateUpstream().blobMetadata(container, name);
        } else {
            // TODO: Find a better way to return errors
            return delegate().blobMetadata(container, name);
        }
    }

    private Blob getBlobMasked(String containerName, String blobName, GetOptions getOptions){
        if(isBlobMasked(containerName, blobName)){
            // TODO: Simlulate an error without doing something like this
            return delegate().getBlob(containerName, "aslkghbfdalkbjhdblkdfhgbdfb");
        }
        if(getOptions == null){
            return delegate().getBlob(containerName, blobName);
        } else {
            return delegate().getBlob(containerName, blobName, getOptions);
        }
    }

    @Override
    public Blob getBlob(String containerName, String blobName) {
        ensureBlobIsLocal(containerName, blobName);
        return getBlobMasked(containerName, blobName, null);
    }

    @Override
    public Blob getBlob(String containerName, String blobName,
                        GetOptions getOptions) {
        ensureBlobIsLocal(containerName, blobName);
        return getBlobMasked(containerName, blobName, getOptions);
    }

    @Override
    public void removeBlob(String container, String name) {
        maskBlob(container, name);
        if(delegate().blobExists(container, name)){
            delegate().removeBlob(container, name);
        }
    }

    @Override
    public void removeBlobs(String container, Iterable<String> iterable) {
        for (String name : iterable) {
            maskBlob(container, name);
            if(delegate().blobExists(container, name)){
                delegate().removeBlob(container, name);
            }
        }
    }

    @Override
    public BlobAccess getBlobAccess(String container, String name) {
        throw new NotImplementedException();
    }

    @Override
    public void setBlobAccess(String container, String name,
                              BlobAccess access) {
        throw new NotImplementedException();
    }

    @Override
    public long countBlobs(String container) {
        return delegate().countBlobs(container);
    }

    @Override
    public long countBlobs(String container, ListContainerOptions options) {
        return delegate().countBlobs(container, options);
    }

    @Override
    public MultipartUpload initiateMultipartUpload(String container, BlobMetadata blobMetadata, PutOptions options) {
        // TODO: Simulate error when file already exists in upstreamContainer
        return delegate().initiateMultipartUpload(container, blobMetadata, options);
    }

    @Override
    public void abortMultipartUpload(MultipartUpload mpu) {
        delegate().abortMultipartUpload(mpu);
    }

    @Override
    public String completeMultipartUpload(MultipartUpload mpu, List<MultipartPart> parts) {
        return delegate().completeMultipartUpload(mpu, parts);
    }

    @Override
    public MultipartPart uploadMultipartPart(MultipartUpload mpu, int partNumber, Payload payload) {
        // TODO: Simulate error when file already exists in upstreamContainer
        return delegate().uploadMultipartPart(mpu, partNumber, payload);
    }

    @Override
    public List<MultipartPart> listMultipartUpload(MultipartUpload mpu) {
        return delegate().listMultipartUpload(mpu);
    }

    @Override
    public List<MultipartUpload> listMultipartUploads(String container) {
        return delegate().listMultipartUploads(container);
    }

    @Override
    public long getMinimumMultipartPartSize() {
        return delegate().getMinimumMultipartPartSize();
    }

    @Override
    public long getMaximumMultipartPartSize() {
        return delegate().getMaximumMultipartPartSize();
    }

    @Override
    public int getMaximumNumberOfParts() {
        return delegate().getMaximumNumberOfParts();
    }

    @Override
    public void downloadBlob(String container, String name, File destination) {
        ensureBlobIsLocal(container, name);
        delegate().downloadBlob(container, name, destination);
    }

    @Override
    public void downloadBlob(String container, String name, File destination, ExecutorService executor) {
        ensureBlobIsLocal(container, name);
        delegate().downloadBlob(container, name, destination, executor);
    }

    @Override
    public InputStream streamBlob(String container, String name) {
        return delegate().streamBlob(container, name);
    }

    @Override
    public InputStream streamBlob(String container, String name, ExecutorService executor) {
        return delegate().streamBlob(container, name, executor);
    }



    private boolean ensureLocalContainerExistsIfUpstreamDoes(String container) {
        if(delegate().containerExists(container)){
            return true;
        } else {
            if(delegateUpstream().containerExists(container)){
                return delegate().createContainerInLocation(null, container);
            }
        }
        return false;
    }

    // Returns true if the provided Metadata is for a Maskfile
    private boolean isBlobMaskFile(StorageMetadata sm){
        return sm.getName().endsWith(this.maskSuffix);
    }

    // Returns the name of the Blob that a Maskfile belongs to
    private String getMaskedBlobFileName(String maskFileName){
        return maskFileName.replace(this.maskSuffix, "");
    }

    // Returns the Maskfile name for the provided Blob name
    private String getBlobMaskFileName(String name){
        return name + this.maskSuffix;
    }

    // Returns true if a Maskfile exists for the provided Blob
    private boolean isBlobMasked(String container, String name){
        if(delegate().containerExists(container)){
            return delegate().blobExists(container, getBlobMaskFileName(name));
        } else {
            return false;
        }
    }

    // Creates a Maskfile for the specified Blob
    private void maskBlob(String container, String name){
        if(isBlobMasked(container, name)){
            logger.debug("[maskBlob]: Blob " + container + "/" + name + " already masked");
            return;
        } else {
            BlobBuilder blobBuilder = blobBuilder(getBlobMaskFileName(name)).payload("");
            delegate().putBlob(container, blobBuilder.build());
            logger.debug("[maskBlob]: Blob " + container + "/" + name + " successfully masked");
        }
    }

    // Removes the Maskfile for the specified Blob
    private void unmaskBlob(String container, String name){
        if(isBlobMasked(container, name)){
            delegate().removeBlob(container, getBlobMaskFileName(name));
            logger.debug("[unmaskBlob]: Blob " + container + "/" + name + " successfully unmasked");
            return;
        } else {
            logger.debug("[unmaskBlob]: Blob " + container + "/" + name + " is not masked");
        }
    }

    // Returns true if the specified Blob is available in the local backend
    private boolean isBlobLocal(String container, String name){
        return delegate().blobExists(container, name);
    }

    // Returns true if it was successful in retrieving a file from the upstream backend
    // Returns false if the file did not exist
    // TODO: Use exceptions instead of Boolean
    private boolean ensureBlobIsLocal(String container, String name){
        if(isBlobLocal(container, name)){
            logger.debug("[ensureBlobIsLocal]: Blob " + container + "/" + name + " is locally available");
            return true;
        } else {
            if(delegateUpstream().blobExists(container, name)){
                logger.debug("[ensureBlobIsLocal]: Blob " + container + "/" + name + " is locally unavailable, and exists in upstream");
                delegate().putBlob(container, delegateUpstream().getBlob(container, name));
                logger.debug("[ensureBlobIsLocal]: Blob " + container + "/" + name + " successfully copied to local storage");
                return true;
            } else {
                logger.warn("[ensureBlobIsLocal]: Blob " + container + "/" + name + " is locally unavailable, and does not exist upstream");
                return false;
            }
        }
    }

    private PageSet<? extends StorageMetadata> mergeAndFilterList(PageSet<StorageMetadata> localSet, PageSet<StorageMetadata> upstreamSet){
        List<String> maskedBlobNames = new ArrayList<String>();

        // TODO: Look at a more performant way of doing this
        // Build a list of masked blobs and remove the maskfiles themselves
        for(StorageMetadata sm : localSet){
            if(isBlobMaskFile(sm)){
                String maskedFile = getMaskedBlobFileName(sm.getName());
                logger.info("[mergeAndFilterList]: Blob " + sm.getName() + " is a maskfile for " + maskedFile);
                maskedBlobNames.add(maskedFile);
                localSet.remove(sm);
            }
        }
        localSet.addAll(upstreamSet);
        // Remove any masked files from the merged list
        for (Iterator<StorageMetadata> iterator = localSet.iterator(); iterator.hasNext();) {
            StorageMetadata sm = iterator.next();
            if(maskedBlobNames.contains(sm.getName())){
                logger.warn("[mergeAndFilterList]: Blob " + sm.getName() + " is masked, removing from list.");
                iterator.remove();
            }
        }
        return localSet;
    }

}
