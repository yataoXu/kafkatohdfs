package com.evan.service.impl;

import com.evan.config.HadoopTemplate;
import com.evan.service.HdfsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description
 * @ClassName HdfsServiceImpl
 * @Author Evan
 * @date 2019.10.15 14:16
 */
@Service
public class HdfsServiceImpl implements HdfsService {

    @Autowired
    private HadoopTemplate hadoopTemplate;

    @Override
    public void uploadFile(String srcFile) {
        hadoopTemplate.uploadFile(srcFile);
    }

    @Override
    public void delFile(String fileName) {
        hadoopTemplate.delFile(fileName);
    }

    @Override
    public void downFile(String fileName, String savePath) {
        hadoopTemplate.download(fileName, savePath);
    }
}
