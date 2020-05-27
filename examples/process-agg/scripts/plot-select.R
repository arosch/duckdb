library(ggplot2)

# command line arguments
args = commandArgs(trailingOnly=TRUE)
if (length(args) != 2) {
  stop("Rscript efficiency-plot.R <data file> <output file>", call.=FALSE)
}

# import csv data
dataFile <- args[1]
df <- read.table(dataFile, header=TRUE, sep=",", numerals="warn.loss")

pplot <- ggplot(data=df, aes(x=selectivity, y=time.sec, group=approach, color=approach, shape = approach))
pplot + geom_point() +
    stat_summary(fun.y = mean, geom = "line") +
    labs (x="Qualified Cases", y="Execution Time") +
    scale_x_continuous(breaks = c(10, 30, 50, 70, 90), labels = c("10%", "30%", "50%", "70%", "90%")) +
    scale_y_continuous(breaks = c(0, 5, 10, 15), labels = c("0s", "5s", "10s", "15s")) +
    #scale_y_continuous(breaks = c(0, 0.5, 0.8, 1.2, 1.6), labels = c("0s", "0.5s", "0.8s", "1.2s", "1.6s")) +
    theme_bw() +
    scale_color_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values=c("#619CFF", "#F8766D", "#00BA38")) + 
    scale_shape_manual(breaks = c("StringAgg", "ArrayAgg", "ShaAgg"), values = c(16, 17, 15)) +
    theme(legend.title = element_blank(), legend.position = c(.01, .99), legend.justification = c("left", "top"), legend.box.just = "left") 

# save to output file
ofile <- args[2]
ggsave(ofile, width=3, height=2.5, dpi=300)
