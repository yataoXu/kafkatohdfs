package com.evan.service;

public interface HdfsService {

    void uploadFile(String srcFile);

    void delFile(String fileName);

    void downFile(String fileName,String savePath);

}
