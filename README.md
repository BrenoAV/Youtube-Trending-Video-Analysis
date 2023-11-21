# PySpark — YouTube Trending Video Analysis


# Interesting Points

1. The best time to release a video is 12:00 PM (Lunch time).
2. The “worst” day to release a video is on Saturday.
3. Tags apparently are not required to video go to trending state.
4. For Brazil: The football (soccer) is incredible consumed until nowadays and minecraft contents.
5. There are some correlations with the number of views with the likes, dislikes, and number of comments. The most correlated are the likes.
6. Put a lot of exclamation mark in the title.

# Summary

Analysis of the YouTube trending video using the dataset provided by [Rishav Sharma - Kaggle](https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset). For that propose was used [PySpark](https://spark.apache.org/docs/latest/api/python/index.html) to process the data. The [Jupyter Notebok](./YoutubeTrendingVideoAnalysis.ipynb) contains my analysis of the data and can be done more analysis on top of.

Note: If you want another country, change the [config.yml](./config.yml) file.

![Image of a laptop and a tablet with the YouTube logo](assets/youtube.jpg)

 <p class="attribution">"<a target="_blank" rel="noopener noreferrer" href="https://www.flickr.com/photos/24614969@N04/9935521594">Youtube</a>" by <a target="_blank" rel="noopener noreferrer" href="https://www.flickr.com/photos/24614969@N04">clasesdeperiodismo</a> is licensed under <a target="_blank" rel="noopener noreferrer" href="https://creativecommons.org/licenses/by-sa/2.0/?ref=openverse">CC BY-SA 2.0 <img src="https://mirrors.creativecommons.org/presskit/icons/cc.svg" style="height: 1em; margin-right: 0.125em; display: inline;"></img><img src="https://mirrors.creativecommons.org/presskit/icons/by.svg" style="height: 1em; margin-right: 0.125em; display: inline;"></img><img src="https://mirrors.creativecommons.org/presskit/icons/sa.svg" style="height: 1em; margin-right: 0.125em; display: inline;"></img></a>. </p>

 # Kaggle

 You can use the [Kaggle Public API](https://www.kaggle.com/docs/api) or just download manually the dataset from the website.

 # Python Dependencies

```console
$ pip install --user pyspark pandas matplotlib seaborn PyYaml kaggle
```

Alternative (container): https://github.com/jupyter/docker-stacks/tree/main/images/pyspark-notebook

---

<p align='center'>
<strong>MIT License</strong><br>
This project is licensed under the MIT License - see the LICENSE file for details.<br><br>
&copy; 2023 BrenoAV
</p>