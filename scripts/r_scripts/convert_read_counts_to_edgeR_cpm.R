#!/usr/bin/env Rscript

args = commandArgs(trailingOnly=TRUE)

if (length(args)!=3) {
  stop("Required params: input_file, output_file and png_plot.", call.=FALSE)
}

library(edgeR)
x <- read.csv(args[1],header=TRUE,row.name='gene_name')
y <- DGEList(counts=x)
y <- calcNormFactors(y)
cpm_count <- cpm(y,log=TRUE)
write.csv(cpm_count,args[2])
png(args[3])
plotMDS(y)
dev.off()