library(ggplot2)

# command line arguments
args = commandArgs(trailingOnly=TRUE)
if (length(args) != 2) {
  stop("Rscript efficiency-plot.R <data file> <output file>", call.=FALSE)
}

# import csv data
dataFile <- args[1]
df <- read.table(dataFile, header=TRUE, sep=",", numerals="warn.loss")

pplot <- ggplot(data=df, aes(x=tuples, y=time.sec, group=approach, color=approach, shape = approach))
pplot + geom_point() +
    stat_summary(fun.y = mean, geom = "line") +
    labs (x="Number of Tuples in million", y="Execution Time") +
    scale_x_continuous(breaks = c(0, 200000000, 400000000, 600000000), labels = c("0", "200", "400", "600")) +
    scale_y_continuous(breaks = c(0, 3, 6, 9), labels = c("0s", "3s", "6s", "9s")) +
    theme_bw() +
    theme(strip.background =element_rect(fill="white"), legend.title = element_blank()) +
    scale_color_manual(breaks = c("ShaAgg-Readable", "ArrayAgg", "ShaAgg"), values=c("#619CFF", "#F8766D", "#C77CFF")) + 
    scale_shape_manual(breaks = c("ShaAgg-Readable", "ArrayAgg", "ShaAgg"), values = c(16, 17, 15))

# save to output file
ofile <- args[2]
ggsave(ofile, width=4.5, height=2.5, dpi=300)
